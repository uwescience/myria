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

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.parallel.ExchangeData.MetaMessage;
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
    DATAMESSAGE_PROCESSING : switch (dm.getType()) {
      case NORMAL:
      case EOI:
      case EOS:
        ConsumerChannel cc = ecp.getInputChannel();
        Consumer msgOwnerOp = cc.getOwnerConsumer();
        ExchangePairID msgOwnerOpID = msgOwnerOp.getOperatorID();
        Schema msgOwnerOpSchema = msgOwnerOp.getSchema();
        QuerySubTreeTask msgOwnerTask = cc.getOwnerTask();
        InputBuffer<TupleBatch, ExchangeData> msgDestIB = msgOwnerOp.getInputBuffer();

        if (msgDestIB == null) {
          // This happens at the iteration input child, because IDBInput emits EOS without EOS input from the
          // iteration
          // child. The driving task of IDBInput will terminate and cleanup before the iteration input child
          // receives
          // an EOS from the final EOS iteration. It doesn't affect working but logically incorrect.
          // And if the destination task failed or got killed.
          if (cc.getOwnerTask().isFinished()) {
            // The processing query already ends, drop the messages.
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Drop Data messge because the destination operator already ends.: {}", dm);
            }
          } else {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error("Operator inputbuffer is null when receiving message " + dm
                  + ". The destination operator is : " + msgOwnerOp + ".", new NullPointerException(
                  "Operator inputbuffer is null"));
            }
          }
          pushToBufferSucceed = true;
          break DATAMESSAGE_PROCESSING;
        }

        ExchangeData ed = null;
        switch (dm.getType()) {
          case NORMAL:

            final List<ColumnMessage> columnMessages = dm.getColumnsList();
            final Column<?>[] columnArray = new Column<?>[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm, dm.getNumTuples());
            }
            final List<Column<?>> columns = Arrays.asList(columnArray);

            ed = new ExchangeData(msgOwnerOpID, remoteID, columns, msgOwnerOpSchema, dm.getNumTuples(), dm.getSeq());
            msgOwnerTask.notifyNewInput();
            break;
          case EOI:
            ed = new ExchangeData(msgOwnerOpID, remoteID, msgOwnerOpSchema, MetaMessage.EOI);
            break;

          case EOS:

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("EOS message @ WorkerDataHandler; worker[" + remoteID + "]; opID["
                  + cc.getOwnerConsumer().getOperatorID() + "]");
            }

            ed = new ExchangeData(msgOwnerOpID, remoteID, msgOwnerOpSchema, MetaMessage.EOS);
            break;
        }
        pushToBufferSucceed = msgDestIB.offer(ed);
        msgOwnerTask.notifyNewInput();
        break;
      case BOS:
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
            LOGGER.error(
                "In receiving message {}, unknown query id: {}, current active queries are: {}, query contained? {}",
                qm, queryId, ownerWorker.getActiveQueries().keySet(), ownerWorker.getActiveQueries().get(queryId));
          }
        } else {
          switch (qm.getType()) {
            case QUERY_START:
              q.init();
              q.startExecution();
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
