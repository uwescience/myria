package edu.washington.escience.myria.parallel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.parallel.Worker.QueryCommand;
import edu.washington.escience.myria.parallel.ipc.IPCMessage;
import edu.washington.escience.myria.parallel.ipc.ShortMessageProcessor;
import edu.washington.escience.myria.proto.ControlProto.ControlMessage;
import edu.washington.escience.myria.proto.QueryProto.QueryMessage;
import edu.washington.escience.myria.proto.QueryProto.QueryMessage.Type;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.util.AttachmentableAdapter;

/**
 * Message handler for workers.
 * */
@Sharable
public final class WorkerShortMessageProcessor extends AttachmentableAdapter
    implements ShortMessageProcessor<TransportMessage> {

  /**
   * The logger for this class.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerShortMessageProcessor.class);

  /**
   * * The worker who owns this handler. *
   * */
  private final Worker ownerWorker;

  /**
   * @param ownerWorker the owner worker.
   * */
  WorkerShortMessageProcessor(final Worker ownerWorker) {
    this.ownerWorker = ownerWorker;
  }

  @Override
  public boolean processMessage(final Channel ch, final IPCMessage.Data<TransportMessage> message) {
    Object o = message.getPayload();
    if (!(o instanceof TransportMessage)) {
      return true;
    }
    TransportMessage tm = (TransportMessage) o;
    if (tm.getType() == TransportMessage.Type.QUERY) {
      return processQueryMessage(ch, message.getRemoteID(), tm.getQueryMessage());
    } else if (tm.getType() == TransportMessage.Type.CONTROL) {
      return processControlMessage(ch, message.getRemoteID(), tm.getControlMessage());
    }
    if (LOGGER.isErrorEnabled()) {
      LOGGER.error("Unknown message type :" + message);
    }
    return false;
  }

  /**
   * @param ch message channel
   * @param remoteID message source worker
   * @param qm the query message
   * @return if the message is successfully processed.
   * */
  private boolean processQueryMessage(final Channel ch, final int remoteID, final QueryMessage qm) {
    SubQueryId subQueryId = new SubQueryId(qm.getQueryId(), qm.getSubqueryId());
    WorkerSubQuery q = null;

    if (qm.getType() == Type.QUERY_DISTRIBUTE) {
      // new received query.
      ObjectInputStream osis = null;
      try {
        osis =
            new ObjectInputStream(new ByteArrayInputStream(qm.getQuery().getQuery().toByteArray()));
        final SubQueryPlan operators = (SubQueryPlan) (osis.readObject());
        q = new WorkerSubQuery(operators, subQueryId, ownerWorker);
        if (!ownerWorker.getQueryQueue().offer(new QueryCommand(q, qm))) {
          return false;
        }
      } catch (IOException | ClassNotFoundException e) {
        LOGGER.error("Error decoding query", e);
      }
    } else {
      q = ownerWorker.getActiveQueries().get(subQueryId);
      if (q == null) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error(
              "In receiving message {}, unknown query id: {}, current active queries are: {}",
              qm,
              subQueryId,
              ownerWorker.getActiveQueries().keySet());
        }
      } else {
        if (!ownerWorker.getQueryQueue().offer(new QueryCommand(q, qm))) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * @param ch message channel
   * @param remoteID message source worker
   * @param cm the control message
   * @return if the message is successfully processed.
   * */
  private boolean processControlMessage(
      final Channel ch, final int remoteID, final ControlMessage cm) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Control message received: " + cm);
    }
    return ownerWorker.getControlMessageQueue().offer(cm);
  }
}
