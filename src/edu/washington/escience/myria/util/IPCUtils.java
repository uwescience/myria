package edu.washington.escience.myria.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.google.protobuf.ByteString;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.parallel.QueryExecutionStatistics;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.proto.ControlProto.ControlMessage;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.DataMessage;
import edu.washington.escience.myria.proto.QueryProto;
import edu.washington.escience.myria.proto.QueryProto.QueryMessage;
import edu.washington.escience.myria.proto.QueryProto.QueryReport;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;

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

  /** Heartbeat message sent from a worker to tell the master that it is alive. */
  public static final TransportMessage CONTROL_WORKER_HEARTBEAT = TransportMessage.newBuilder().setType(
      TransportMessage.Type.CONTROL).setControlMessage(
      ControlMessage.newBuilder().setType(ControlMessage.Type.WORKER_HEARTBEAT)).build();

  /**
   * @param workerId the id of the worker to be removed.
   * @return the remove worker TM.
   * */
  public static TransportMessage removeWorkerTM(final int workerId) {
    return TransportMessage.newBuilder().setType(TransportMessage.Type.CONTROL).setControlMessage(
        ControlMessage.newBuilder().setType(ControlMessage.Type.REMOVE_WORKER).setWorkerId(workerId)).build();
  }

  /**
   * @param workerId the id of the worker to be removed.
   * @return the remove worker TM.
   * */
  public static TransportMessage removeWorkerAckTM(final int workerId) {
    return TransportMessage.newBuilder().setType(TransportMessage.Type.CONTROL).setControlMessage(
        ControlMessage.newBuilder().setType(ControlMessage.Type.REMOVE_WORKER_ACK).setWorkerId(workerId)).build();
  }

  /**
   * @param workerId the id of the worker to be added.
   * @param socketinfo the SocketInfo of the worker to be added.
   * @return the add worker TM.
   * */
  public static TransportMessage addWorkerTM(final int workerId, final SocketInfo socketinfo) {
    return TransportMessage.newBuilder().setType(TransportMessage.Type.CONTROL).setControlMessage(
        ControlMessage.newBuilder().setType(ControlMessage.Type.ADD_WORKER).setWorkerId(workerId).setRemoteAddress(
            socketinfo.toProtobuf())).build();
  }

  /**
   * @param workerId the id of the worker to be added.
   * @return the add worker TM.
   * */
  public static TransportMessage addWorkerAckTM(final int workerId) {
    return TransportMessage.newBuilder().setType(TransportMessage.Type.CONTROL).setControlMessage(
        ControlMessage.newBuilder().setType(ControlMessage.Type.ADD_WORKER_ACK).setWorkerId(workerId)).build();
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
   * @param queryId .
   * @param workerId .
   * @return a query recover TM.
   * */
  public static TransportMessage recoverQueryTM(final Long queryId, final int workerId) {
    return QUERY_TM_BUILDER.get().setQueryMessage(
        QueryMessage.newBuilder().setType(QueryMessage.Type.QUERY_RECOVER).setQueryId(queryId).setWorkerId(workerId))
        .build();
  }

  /**
   * @param queryId the completed query id.
   * @return a query complete TM.
   * @param statistics query execution statistics
   * */
  public static TransportMessage queryCompleteTM(final long queryId, final QueryExecutionStatistics statistics) {
    return QUERY_TM_BUILDER.get().setQueryMessage(
        QueryMessage.newBuilder().setQueryId(queryId).setType(QueryMessage.Type.QUERY_COMPLETE).setQueryReport(
            QueryReport.newBuilder().setSuccess(true).setExecutionStatistics(statistics.toProtobuf()))).build();
  }

  /**
   * Make sure a {@link Throwable} is serializable. If any of the {@Throwable}s in the given err error
   * hierarchy (The hierarchy formed by caused-by and suppressed) is not serializable, it will be replaced by a
   * {@link DbException}. The stack trace of the original {@link Throwable} is kept.
   * 
   * @param err the {@link Throwable}
   * @return A {@link Throwable} that is guaranteed to be serializable.
   * */
  public static Throwable wrapSerializableThrowable(final Throwable err) {
    if (err == null) {
      return null;
    }
    final ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
    try {
      final ObjectOutputStream oos = new ObjectOutputStream(inMemBuffer);
      oos.writeObject(err);
    } catch (NotSerializableException e) {
      Throwable cause = err.getCause();
      if (cause != null) {
        cause = wrapSerializableThrowable(cause);
      }
      DbException ret = new DbException(err.getMessage(), cause);
      ret.setStackTrace(err.getStackTrace());
      Throwable[] suppressed = err.getSuppressed();
      if (suppressed != null) {
        for (Throwable element : suppressed) {
          ret.addSuppressed(wrapSerializableThrowable(element));
        }
      }
      return ret;
    } catch (IOException e) {
      DbException ret = new DbException(e);
      ret.setStackTrace(err.getStackTrace());
      return ret;
    }
    return err;
  }

  /**
   * @param queryId the completed query id.
   * @return a query complete TM.
   * @param cause the cause of the failure
   * @throws IOException if any IO error occurs.
   * @param statistics query execution statistics
   * */
  public static TransportMessage queryFailureTM(final long queryId, final Throwable cause,
      final QueryExecutionStatistics statistics) throws IOException {
    final ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(inMemBuffer);
    oos.writeObject(wrapSerializableThrowable(cause));
    oos.flush();
    inMemBuffer.flush();
    return QUERY_TM_BUILDER.get().setQueryMessage(
        QueryMessage.newBuilder().setQueryId(queryId).setType(QueryMessage.Type.QUERY_COMPLETE).setQueryReport(
            QueryReport.newBuilder().setSuccess(false).setExecutionStatistics(statistics.toProtobuf()).setCause(
                ByteString.copyFrom(inMemBuffer.toByteArray())))).build();
  }

  /**
   * @param queryId the completed query id.
   * @return a query complete TM.
   * */
  public static TransportMessage simpleQueryFailureTM(final long queryId) {
    return QUERY_TM_BUILDER.get().setQueryMessage(
        QueryMessage.newBuilder().setQueryId(queryId).setType(QueryMessage.Type.QUERY_COMPLETE).setQueryReport(
            QueryReport.newBuilder().setSuccess(false))).build();
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
   * Check if the remote side of the channel is still connected.
   * 
   * @param ch the channel to check.
   * @return true if the remote side is still connected, false otherwise.
   * */
  public static boolean isRemoteConnected(final StreamOutputChannel<?> ch) {
    if (ch == null) {
      return false;
    }
    return isRemoteConnected(ch.getIOChannel());
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
   * @return a {@link TupleBatch} created from a data protobuf.
   * @param dm data message
   * @param s schema
   * */
  public static TupleBatch tmToTupleBatch(final DataMessage dm, final Schema s) {
    switch (dm.getType()) {
      case NORMAL:
        final List<ColumnMessage> columnMessages = dm.getColumnsList();
        final Column<?>[] columnArray = new Column<?>[columnMessages.size()];
        int idx = 0;
        for (final ColumnMessage cm : columnMessages) {
          columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm, dm.getNumTuples());
        }
        final List<Column<?>> columns = Arrays.asList(columnArray);
        return new TupleBatch(s, columns, dm.getNumTuples());
      case EOI:
        return TupleBatch.eoiTupleBatch(s);
      default:
        throw new IllegalArgumentException("Unknown DATA message type: " + dm.getType());
    }
  }

  /**
   * @param queryId .
   * @param query the query to encode.
   * @throws IOException if error occurs in encoding the query.
   * @return an encoded query TM
   */
  public static TransportMessage queryMessage(final long queryId, final SingleQueryPlanWithArgs query)
      throws IOException {
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
