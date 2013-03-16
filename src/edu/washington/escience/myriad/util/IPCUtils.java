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
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage.ControlMessageType;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.QueryProto;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;

/**
 * A bunch of utility methods for IPC layer.
 * */
public final class IPCUtils {

  /**
   * Pause read from the ch Channel, this will back-pressure to TCP layer. The TCP stream control will automatically
   * pause the sending from the remote.
   * 
   * @param ch the Channel.
   * @return the future instance of the pauseing read action
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
          return TransportMessage.newBuilder().setType(TransportMessageType.DATA);
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
          return TransportMessage.newBuilder().setType(TransportMessageType.CONTROL);
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
          return TransportMessage.newBuilder().setType(TransportMessageType.QUERY);
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
          return DataMessage.newBuilder().setType(DataMessageType.NORMAL);
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
          return DataMessage.newBuilder().setType(DataMessageType.EOS);
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
          return DataMessage.newBuilder().setType(DataMessageType.BOS);
        }
      };

  public static final TransportMessage EOS = TransportMessage.newBuilder().setType(TransportMessageType.DATA).setData(
      DataMessage.newBuilder().setType(DataMessageType.EOS)).build();

  public static final TransportMessage EOI = TransportMessage.newBuilder().setType(TransportMessageType.DATA).setData(
      DataMessage.newBuilder().setType(DataMessageType.EOI)).build();

  protected static final ThreadLocal<DataMessage.Builder> EOI_DATAMESSAGE_BUILDER =
      new ThreadLocal<DataMessage.Builder>() {
        @Override
        protected DataMessage.Builder initialValue() {
          return DataMessage.newBuilder().setType(DataMessageType.EOI);
        }
      };

  // public static final TransportMessage CONTROL_QUERY_READY = TransportMessage.newBuilder().setType(
  // TransportMessageType.CONTROL).setControl(
  // ControlMessage.newBuilder().setType(ControlMessageType.QUERY_READY_TO_EXECUTE).build()).build();

  public static final TransportMessage CONTROL_SHUTDOWN = TransportMessage.newBuilder().setType(
      TransportMessageType.CONTROL).setControl(ControlMessage.newBuilder().setType(ControlMessageType.SHUTDOWN))
      .build();

  public static final TransportMessage CONTROL_DISCONNECT = TransportMessage.newBuilder().setType(
      TransportMessageType.CONTROL).setControl(ControlMessage.newBuilder().setType(ControlMessageType.DISCONNECT))
      .build();

  /** Control message sent from a worker to tell the master that it is alive. */
  public static final TransportMessage CONTROL_WORKER_ALIVE = TransportMessage.newBuilder().setType(
      TransportMessageType.CONTROL).setControl(ControlMessage.newBuilder().setType(ControlMessageType.WORKER_ALIVE))
      .build();

  public static TransportMessage asTM(final Object m) {
    return (TransportMessage) m;
  }

  public static Integer checkConnectTM(final TransportMessage message) {
    if (message == null) {
      return null;
    }
    if ((message.getType() == TransportMessage.TransportMessageType.CONTROL)
        && (ControlMessage.ControlMessageType.CONNECT == message.getControl().getType())) {
      return message.getControl().getRemoteId();
    }
    return null;
  }

  /**
   * Only connectTm needs the RemoteID field.
   * */
  public static TransportMessage connectTM(final Integer myID) {
    return CONTROL_TM_BUILDER.get().setControl(
        ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.CONNECT).setRemoteId(myID)).build();
  }

  public static TransportMessage queryReadyTM(final Long queryId) {
    return CONTROL_TM_BUILDER.get().setControl(
        ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.QUERY_READY_TO_EXECUTE).setQueryId(
            queryId)).build();
  }

  public static TransportMessage queryCompleteTM(final Long queryId) {
    return CONTROL_TM_BUILDER.get().setControl(
        ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.QUERY_COMPLETE).setQueryId(queryId))
        .build();
  }

  public static TransportMessage startQueryTM(final Long queryId) {
    return CONTROL_TM_BUILDER.get().setControl(
        ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.START_QUERY).setQueryId(queryId)).build();
  }

  // /**
  // * create an EOS data message.
  // * */
  // public static TransportMessage eosTM(final ExchangePairID epID) {
  // return DATA_TM_BUILDER.get().setData(EOS_DATAMESSAGE_BUILDER.get().setOperatorID(epID.getLong())).build();
  // }
  //
  // public static TransportMessage eoiTM(final ExchangePairID epID) {
  // return DATA_TM_BUILDER.get().setData(EOI_DATAMESSAGE_BUILDER.get().setOperatorID(epID.getLong())).build();
  // }

  /**
   * Check if the remote side of the channel is still connected.
   * 
   * @param channel the channel to check.
   * @return true if the remote side is still connected, false otherwise.
   * */
  public static boolean isRemoteConnected(final Channel channel) {
    if (channel != null) {
      if (!channel.isReadable()) {
        ChannelFuture cf = channel.setInterestOps(Channel.OP_READ);
        if (cf == null) {
          return false;
        } else {
          cf.awaitUninterruptibly();
          if (!cf.isSuccess() || !channel.isReadable()) {
            return false;
          } else {
            return true;
          }
        }
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  /**
   * create an EOS data message.
   * */
  public static TransportMessage bosTM(final ExchangePairID epID) {
    return DATA_TM_BUILDER.get().setData(BOS_DATAMESSAGE_BUILDER.get().setOperatorID(epID.getLong())).build();
  }

  public static TransportMessage normalDataMessage(final List<Column<?>> dataColumns, final int numTuples,
      final long seqNum) {
    final ColumnMessage[] columnProtos = new ColumnMessage[dataColumns.size()];

    int i = 0;
    for (final Column<?> c : dataColumns) {
      columnProtos[i] = c.serializeToProto();
      i++;
    }
    return DATA_TM_BUILDER.get().setData(
        NORMAL_DATAMESSAGE_BUILDER.get().clearColumns().addAllColumns(Arrays.asList(columnProtos)).setNumTuples(
            numTuples)).setSeq(seqNum).build();
  }

  public static TransportMessage queryMessage(final Long queryId, final Operator[] query) throws IOException {
    final ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(inMemBuffer);
    oos.writeObject(query);
    oos.flush();
    inMemBuffer.flush();
    return QUERY_TM_BUILDER.get().setQuery(
        QueryProto.Query.newBuilder().setQueryId(queryId).setQuery(ByteString.copyFrom(inMemBuffer.toByteArray())))
        .build();
  }

  /**
   * util classes are not instantiable.
   * */
  private IPCUtils() {
  }
}
