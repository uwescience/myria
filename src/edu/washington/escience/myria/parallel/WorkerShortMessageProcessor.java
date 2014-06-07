package edu.washington.escience.myria.parallel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.parallel.ipc.IPCMessage;
import edu.washington.escience.myria.parallel.ipc.ShortMessageProcessor;
import edu.washington.escience.myria.proto.ControlProto.ControlMessage;
import edu.washington.escience.myria.proto.QueryProto.QueryMessage;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.util.AttachmentableAdapter;

/**
 * Message handler for workers.
 * */
@Sharable
public final class WorkerShortMessageProcessor extends AttachmentableAdapter implements
    ShortMessageProcessor<TransportMessage> {

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
    TransportMessage tm = message.getPayload();
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
    boolean result = true;
    switch (qm.getType()) {
      case QUERY_START:
      case QUERY_KILL:
      case QUERY_RECOVER:
        q = ownerWorker.getActiveQueries().get(subQueryId);
        if (q == null) {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error("In receiving message {}, unknown query id: {}, current active queries are: {}", qm, subQueryId,
                ownerWorker.getActiveQueries().keySet());
          }
        } else {
          switch (qm.getType()) {
            case QUERY_START:
              q.init();
              q.startExecution();
              break;
            case QUERY_KILL:
              q.kill();
              break;
            case QUERY_RECOVER:
              if (q.getFTMode().equals(FTMODE.rejoin)) {
                q.addRecoveryTasks(qm.getWorkerId());
              }
              break;
            default:
              break;
          }
        }
        break;
      case QUERY_DISTRIBUTE:
        ObjectInputStream osis = null;
        try {
          osis = new ObjectInputStream(new ByteArrayInputStream(qm.getQuery().getQuery().toByteArray()));
          final SubQueryPlan operators = (SubQueryPlan) (osis.readObject());
          q = new WorkerSubQuery(operators, subQueryId, ownerWorker);
          result = ownerWorker.getQueryQueue().offer(q);
          if (!result) {
            break;
          }
          LOGGER.debug("Query received from: {}. {}", remoteID, q);
        } catch (IOException | ClassNotFoundException e) {
          LOGGER.error("Error decoding query", e);
        }
        break;
      default:
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
}
