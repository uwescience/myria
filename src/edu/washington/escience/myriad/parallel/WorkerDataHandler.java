package edu.washington.escience.myriad.parallel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.List;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.parallel.ExchangeData.MetaMessage;
import edu.washington.escience.myriad.parallel.Worker.QueryExecutionMode;
import edu.washington.escience.myriad.parallel.ipc.ChannelContext;
import edu.washington.escience.myriad.parallel.ipc.MessageChannelHandler;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.QueryProto.QueryMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

/**
 * Message handler for workers.
 * */
@Sharable
public final class WorkerDataHandler extends SimpleChannelUpstreamHandler implements
    MessageChannelHandler<TransportMessage> {

  /**
   * The logger for this class.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerDataHandler.class);

  /**
   * * The worker who owns this handler. *
   * */
  private final Worker ownerWorker;

  /**
   * @param ownerWorker the owner worker.
   * */
  WorkerDataHandler(final Worker ownerWorker) {
    this.ownerWorker = ownerWorker;
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
    final TransportMessage tm = (TransportMessage) e.getMessage();
    final Channel channel = e.getChannel();
    final ChannelContext cs = (ChannelContext) channel.getAttachment();
    final ChannelContext.RegisteredChannelContext ecc = cs.getRegisteredChannelContext();
    final Integer senderID = ecc.getRemoteID();
    while (!processMessage(channel, senderID, tm)) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Input buffer out of memory. With the flow control input buffers, it should not happen normally.");
      }
    }
    ctx.sendUpstream(e);
  }

  @Override
  public boolean processMessage(final Channel ch, final int remoteID, final TransportMessage message) {
    switch (message.getType()) {
      case DATA:
        return processDataMessage(ch, remoteID, message.getDataMessage());
      case QUERY:
        return processQueryMessage(ch, remoteID, message.getQueryMessage());
      case CONTROL:
        return processControlMessage(ch, remoteID, message.getControlMessage());
    }
    return false;
  }

  /**
   * @param ch message channel
   * @param remoteID message source worker
   * @param dm the data message
   * @return if the message is successfully processed.
   * */
  private boolean processDataMessage(final Channel ch, final int remoteID, final DataMessage dm) {
    boolean pushToBufferSucceed = true;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("TupleBatch received from " + remoteID + " to Operator: " + dm.getOperatorID());
    }
    final ChannelContext cs = (ChannelContext) ch.getAttachment();
    ExchangeChannelPair ecp = (ExchangeChannelPair) cs.getAttachment();
    switch (dm.getType()) {
      case NORMAL:
        ConsumerChannel cc = ecp.getInputChannel();
        Consumer op = cc.getOwnerConsumer();
        final List<ColumnMessage> columnMessages = dm.getColumnsList();
        final Column<?>[] columnArray = new Column<?>[columnMessages.size()];
        int idx = 0;
        for (final ColumnMessage cm : columnMessages) {
          columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm, dm.getNumTuples());
        }
        final List<Column<?>> columns = Arrays.asList(columnArray);

        pushToBufferSucceed =
            op.getInputBuffer()
                .offer(
                    new ExchangeData(op.getOperatorID(), remoteID, columns, op.getSchema(), dm.getNumTuples(), dm
                        .getSeq()));
        cc.getOwnerTask().notifyNewInput();
        break;
      case EOI:
        cc = ecp.getInputChannel();
        op = cc.getOwnerConsumer();
        if (op.getInputBuffer() == null) {
          if (cc.getOwnerTask().isFinished()) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Iteration EOI input for iteration input of IDBInput. Drop directly.");
            }
          } else {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error("Operator inputbuffer is null.", new NullPointerException("Operator inputbuffer is null"));
            }
          }
        } else {
          pushToBufferSucceed =
              op.getInputBuffer()
                  .offer(new ExchangeData(op.getOperatorID(), remoteID, op.getSchema(), MetaMessage.EOI));
          cc.getOwnerTask().notifyNewInput();
        }
        break;
      case BOS:
        break;
      case EOS:
        cc = ecp.getInputChannel();
        // if (cc != null) {
        // eosReceived.put(ch.getId(), cc.op);
        // }
        // if (cc == null) {
        // Consumer supposed = eosReceived.get(ch.getId());
        // LOGGER.debug("" + supposed);
        // } else if (cc.op == null) {
        // LOGGER.debug("");
        // }

        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("EOS message @ WorkerDataHandler; worker[" + remoteID + "]; opID["
              + cc.getOwnerConsumer().getOperatorID() + "]");
        }

        op = cc.getOwnerConsumer();
        if (op.getInputBuffer() == null) {
          // This happens at the iteration input child, because IDBInput emits EOS without EOS input from the
          // iteration
          // child. The driving task of IDBInput will terminate and cleanup before the iteration input child
          // receives
          // an EOS from the final EOS iteration. It doesn't affect working but logically incorrect.
          // TODO refactoring of the operator interface may solve this.
          if (cc.getOwnerTask().isFinished()) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Iteration EOS input for iteration input of IDBInput. Drop directly.");
            }
          } else {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error("Operator inputbuffer is null.", new NullPointerException("Operator inputbuffer is null"));
            }
          }
        } else {
          pushToBufferSucceed =
              op.getInputBuffer()
                  .offer(new ExchangeData(op.getOperatorID(), remoteID, op.getSchema(), MetaMessage.EOS));
          cc.getOwnerTask().notifyNewInput();
        }
        break;
    }
    return pushToBufferSucceed;
  }

  /**
   * @param ch message channel
   * @param remoteID message source worker
   * @param qm the query message
   * @return if the message is successfully processed.
   * */
  private boolean processQueryMessage(final Channel ch, final int remoteID, final QueryMessage qm) {
    long queryId = qm.getQueryId();
    WorkerQueryPartition q = null;
    boolean result = true;
    switch (qm.getType()) {
      case QUERY_START:
      case QUERY_PAUSE:
      case QUERY_RESUME:
      case QUERY_KILL:
        q = ownerWorker.getActiveQueries().get(queryId);
        if (q == null) {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Unknown query id: {}, current active queries are: {}", queryId, ownerWorker
                .getActiveQueries().keySet());
          }
        } else {
          switch (qm.getType()) {
            case QUERY_START:
              q.init();
              if (ownerWorker.getQueryExecutionMode() == QueryExecutionMode.NON_BLOCKING) {
                q.startNonBlockingExecution();
              } else {
                q.startBlockingExecution();
              }
              break;
            case QUERY_PAUSE:
              q.pause();
              break;
            case QUERY_RESUME:
              q.resume();
              break;
            case QUERY_KILL:
              q.kill();
              break;
          }
        }
        break;
      case QUERY_DISTRIBUTE:
        ObjectInputStream osis = null;
        try {
          osis = new ObjectInputStream(new ByteArrayInputStream(qm.getQuery().getQuery().toByteArray()));
          final RootOperator[] operators = (RootOperator[]) (osis.readObject());
          q = new WorkerQueryPartition(operators, queryId, ownerWorker);
          result = ownerWorker.getQueryQueue().offer(q);
          if (!result) {
            break;
          }
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Query received from: " + remoteID + ". " + q);
          }
        } catch (IOException | ClassNotFoundException e) {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Error decoding query", e);
          }
        }
        break;
    }
    return result;
  }

  /**
   * @param ch message channel
   * @param remoteID message source worker
   * @param cm the control message
   * @return if the message is successfully processed.
   * */
  private boolean processControlMessage(final Channel ch, final int remoteID, final ControlMessage cm) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Control message received: " + cm);
    }
    return ownerWorker.getControlMessageQueue().offer(cm);
  }
  // for debugging
  // public final java.util.concurrent.ConcurrentHashMap<Integer, Consumer> eosReceived =
  // new ConcurrentHashMap<Integer, Consumer>();
}
