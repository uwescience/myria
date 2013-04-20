package edu.washington.escience.myriad.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.QueryProto;
import edu.washington.escience.myriad.proto.QueryProto.QueryMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

/**
 * A bunch of utility methods for IPC layer.
 * */
public final class IPCUtils {

  /**
   * Pause read from the ch Channel, this will back-pressure to TCP layer. The TCP stream control will automatically
   * pause the sending from the remote.
   * 
   * @param ch the Channel.
   * @return the future instance of the pausing read action
   * */
  public static ChannelFuture pauseRead(final Channel ch) {
    return ch.setReadable(false);
  }

  /**
   * Resume read.
   * 
   * @param ch the Channel
   * @return the future instance of the resuming read action
   * */
  public static ChannelFuture resumeRead(final Channel ch) {
    return ch.setReadable(true);
  }

  /**
   * Thread local TransportMessage builder. May reduce the cost of creating builder instances.
   * 
   * @return builder.
   * */
  protected static final ThreadLocal<TransportMessage.Builder> DATA_TM_BUILDER =
      new ThreadLocal<TransportMessage.Builder>() {
        @Override
        protected TransportMessage.Builder initialValue() {
          return TransportMessage.newBuilder().setType(TransportMessage.Type.DATA);
        }
      };

  /**
   * Thread local TransportMessage builder. May reduce the cost of creating builder instances.
   * 
   * @return builder.
   * */
  protected static final ThreadLocal<TransportMessage.Builder> CONTROL_TM_BUILDER =
      new ThreadLocal<TransportMessage.Builder>() {
        @Override
        protected TransportMessage.Builder initialValue() {
          return TransportMessage.newBuilder().setType(TransportMessage.Type.CONTROL);
        }
      };

  /**
   * Thread local TransportMessage builder. May reduce the cost of creating builder instances.
   * 
   * @return builder.
   * */
  protected static final ThreadLocal<TransportMessage.Builder> QUERY_TM_BUILDER =
      new ThreadLocal<TransportMessage.Builder>() {
        @Override
        protected TransportMessage.Builder initialValue() {
          return TransportMessage.newBuilder().setType(TransportMessage.Type.QUERY);
        }
      };

  /**
   * Thread local DataMessage builder. May reduce the cost of creating builder instances.
   * 
   * @return builder.
   * */
  protected static final ThreadLocal<DataMessage.Builder> NORMAL_DATAMESSAGE_BUILDER =
      new ThreadLocal<DataMessage.Builder>() {
        @Override
        protected DataMessage.Builder initialValue() {
          return DataMessage.newBuilder().setType(DataMessage.Type.NORMAL);
        }
      };

  /**
   * Thread local EOS DataMessage builder. May reduce the cost of creating builder instances.
   * 
   * @return builder.
   * */
  protected static final ThreadLocal<DataMessage.Builder> EOS_DATAMESSAGE_BUILDER =
      new ThreadLocal<DataMessage.Builder>() {
        @Override
        protected DataMessage.Builder initialValue() {
          return DataMessage.newBuilder().setType(DataMessage.Type.EOS);
        }
      };

  /**
   * Thread local EOS DataMessage builder. May reduce the cost of creating builder instances.
   * 
   * @return builder.
   * */
  protected static final ThreadLocal<DataMessage.Builder> BOS_DATAMESSAGE_BUILDER =
      new ThreadLocal<DataMessage.Builder>() {
        @Override
        protected DataMessage.Builder initialValue() {
          return DataMessage.newBuilder().setType(DataMessage.Type.BOS);
        }
      };

  /**
   * EOS TM.
   * */
  public static final TransportMessage EOS = TransportMessage.newBuilder().setType(TransportMessage.Type.DATA)
      .setDataMessage(DataMessage.newBuilder().setType(DataMessage.Type.EOS)).build();

  /**
   * EOI TM.
   * */
  public static final TransportMessage EOI = TransportMessage.newBuilder().setType(TransportMessage.Type.DATA)
      .setDataMessage(DataMessage.newBuilder().setType(DataMessage.Type.EOI)).build();

  /**
   * EOI builder.
   * */
  protected static final ThreadLocal<DataMessage.Builder> EOI_DATAMESSAGE_BUILDER =
      new ThreadLocal<DataMessage.Builder>() {
        @Override
        protected DataMessage.Builder initialValue() {
          return DataMessage.newBuilder().setType(DataMessage.Type.EOI);
        }
      };

  /**
   * shutdown TM.
   * */
  public static final TransportMessage CONTROL_SHUTDOWN = TransportMessage.newBuilder().setType(
      TransportMessage.Type.CONTROL).setControlMessage(
      ControlMessage.newBuilder().setType(ControlMessage.Type.SHUTDOWN)).build();

  /**
   * disconnect TM.
   * */
  public static final TransportMessage CONTROL_DISCONNECT = TransportMessage.newBuilder().setType(
      TransportMessage.Type.CONTROL).setControlMessage(
      ControlMessage.newBuilder().setType(ControlMessage.Type.DISCONNECT)).build();

  /** Control message sent from a worker to tell the master that it is alive. */
  public static final TransportMessage CONTROL_WORKER_ALIVE = TransportMessage.newBuilder().setType(
      TransportMessage.Type.CONTROL).setControlMessage(
      ControlMessage.newBuilder().setType(ControlMessage.Type.WORKER_ALIVE)).build();

  /**
   * Check if the message is a CONNECT message.
   * 
   * @return null if it's not, or the remoteID if it is.
   * @param message the message to check
   * */
  public static Integer checkConnectTM(final TransportMessage message) {
    if (message == null) {
      return null;
    }
    if ((message.getType() == TransportMessage.Type.CONTROL)
        && (ControlMessage.Type.CONNECT == message.getControlMessage().getType())) {
      return message.getControlMessage().getRemoteId();
    }
    return null;
  }

  /**
   * Only connectTm needs the RemoteID field.
   * 
   * @param myID my IPC ID.
   * @return a connect TM.
   * */
  public static TransportMessage connectTM(final int myID) {
    return CONTROL_TM_BUILDER.get().setControlMessage(
        ControlMessage.newBuilder().setType(ControlMessage.Type.CONNECT).setRemoteId(myID)).build();
  }

  /**
   * @param queryId .
   * @return a query ready TM.
   * */
  public static TransportMessage queryReadyTM(final Long queryId) {
    return QUERY_TM_BUILDER.get().setQueryMessage(
        QueryMessage.newBuilder().setType(QueryMessage.Type.QUERY_READY_TO_EXECUTE).setQueryId(queryId)).build();
  }

  /**
   * @param queryId the completed query id.
   * @return a query complete TM.
   * */
  public static TransportMessage queryCompleteTM(final long queryId) {
    return QUERY_TM_BUILDER.get().setQueryMessage(
        QueryMessage.newBuilder().setType(QueryMessage.Type.QUERY_COMPLETE).setQueryId(queryId)).build();
  }

  /**
   * @param queryId the id of the query to be started.
   * @return the query start TM.
   * */
  public static TransportMessage startQueryTM(final long queryId) {
    return QUERY_TM_BUILDER.get().setQueryMessage(
        QueryMessage.newBuilder().setType(QueryMessage.Type.QUERY_START).setQueryId(queryId)).build();
  }

  /**
   * Check if the remote side of the channel is still connected.
   * 
   * @param channel the channel to check.
   * @return true if the remote side is still connected, false otherwise.
   * */
  public static boolean isRemoteConnected(final Channel channel) {
    if (channel != null) {
      if (!channel.isReadable()) {
        channel.setInterestOps(Channel.OP_READ).awaitUninterruptibly();
        return channel.isReadable();
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  /**
   * @return create an BOS data message.
   * @param epID BOS for this operator ID.
   * */
  public static TransportMessage bosTM(final ExchangePairID epID) {
    return DATA_TM_BUILDER.get().setDataMessage(BOS_DATAMESSAGE_BUILDER.get().setOperatorID(epID.getLong())).build();
  }

  /**
   * @param dataColumns data columns
   * @param numTuples number of tuples in the columns.
   * @return a data TM encoding the data columns.
   * */
  public static TransportMessage normalDataMessage(final List<Column<?>> dataColumns, final int numTuples) {
    final ColumnMessage[] columnProtos = new ColumnMessage[dataColumns.size()];

    int i = 0;
    for (final Column<?> c : dataColumns) {
      columnProtos[i] = c.serializeToProto();
      i++;
    }
    return DATA_TM_BUILDER.get().setDataMessage(
        NORMAL_DATAMESSAGE_BUILDER.get().clearColumns().addAllColumns(Arrays.asList(columnProtos)).setNumTuples(
            numTuples)).build();
  }

  /**
   * @param queryId .
   * @param query the query to encode.
   * @throws IOException if error occurs in encoding the query.
   * @return an encoded query TM
   */
  public static TransportMessage queryMessage(final long queryId, final RootOperator[] query) throws IOException {
    final ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(inMemBuffer);
    oos.writeObject(query);
    oos.flush();
    inMemBuffer.flush();
    return QUERY_TM_BUILDER.get().setQueryMessage(
        QueryMessage.newBuilder().setType(QueryMessage.Type.QUERY_DISTRIBUTE).setQueryId(queryId).setQuery(
            QueryProto.Query.newBuilder().setQuery(ByteString.copyFrom(inMemBuffer.toByteArray())))).build();
  }

  /**
   * @param queryId .
   * @return a query ready TM.
   * */
  public static TransportMessage killQueryTM(final long queryId) {
    return QUERY_TM_BUILDER.get().setQueryMessage(
        QueryMessage.newBuilder().setType(QueryMessage.Type.QUERY_KILL).setQueryId(queryId)).build();
  }

  /**
   * util classes are not instantiable.
   * */
  private IPCUtils() {
  }
}
