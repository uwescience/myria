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
import edu.washington.escience.myriad.parallel.ipc.ChannelContext;
import edu.washington.escience.myriad.parallel.ipc.MessageChannelHandler;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerDataHandler.class.getName());

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
    boolean pushToBufferSucceed = true;
    switch (message.getType()) {
      case DATA:
        final DataMessage data = message.getData();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("TupleBatch received from " + remoteID + " to Operator: " + data.getOperatorID());
        }
        final ChannelContext cs = (ChannelContext) ch.getAttachment();
        ExchangeChannelPair ecp = (ExchangeChannelPair) cs.getAttachment();
        switch (data.getType()) {
          case NORMAL:
            ConsumerChannel cc = ecp.getInputChannel();
            final List<ColumnMessage> columnMessages = data.getColumnsList();
            final Column<?>[] columnArray = new Column[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm, data.getNumTuples());
            }
            final List<Column<?>> columns = Arrays.asList(columnArray);

            pushToBufferSucceed =
                cc.op.inputBuffer.offer(new ExchangeData(cc.op.getOperatorID(), remoteID, columns, cc.op.getSchema(),
                    data.getNumTuples(), message.getSeq()));
            cc.ownerTask.notifyNewInput();
            break;
          case EOI:
            cc = ecp.getInputChannel();
            pushToBufferSucceed =
                cc.op.inputBuffer.offer(new ExchangeData(cc.op.getOperatorID(), remoteID, cc.op.getSchema(),
                    MetaMessage.EOI));
            cc.ownerTask.notifyNewInput();
            break;
          case BOS:
            break;
          case EOS:
            cc = ecp.getInputChannel();
            pushToBufferSucceed =
                cc.op.inputBuffer.offer(new ExchangeData(cc.op.getOperatorID(), remoteID, cc.op.getSchema(),
                    MetaMessage.EOS));
            cc.ownerTask.notifyNewInput();
            break;
        }
        break;
      case QUERY:

        ObjectInputStream osis;
        try {
          long id = message.getQuery().getQueryId();
          osis = new ObjectInputStream(new ByteArrayInputStream(message.getQuery().getQuery().toByteArray()));
          final RootOperator[] operators = (RootOperator[]) (osis.readObject());
          WorkerQueryPartition q = new WorkerQueryPartition(operators, id, ownerWorker);
          ownerWorker.queryQueue.offer(q);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Query received from: " + remoteID + ". " + q);
          }
        } catch (IOException | ClassNotFoundException e) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Error decoding query", e);
          }
        }
        break;
      case CONTROL:
        final ControlMessage controlM = message.getControl();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Control message received: " + controlM);
        }
        ownerWorker.controlMessageQueue.offer(controlM);
        break;
    }
    return pushToBufferSucceed;
  }
}
