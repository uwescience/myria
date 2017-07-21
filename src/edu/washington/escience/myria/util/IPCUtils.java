package edu.washington.escience.myria.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.google.protobuf.ByteString;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.parallel.ExecutionStatistics;
import edu.washington.escience.myria.parallel.ResourceStats;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.parallel.SubQueryId;
import edu.washington.escience.myria.parallel.SubQueryPlan;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.proto.ControlProto;
import edu.washington.escience.myria.proto.ControlProto.ControlMessage;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.DataMessage;
import edu.washington.escience.myria.proto.QueryProto;
import edu.washington.escience.myria.proto.QueryProto.QueryMessage;
import edu.washington.escience.myria.proto.QueryProto.QueryReport;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A bunch of utility methods for IPC layer.
 * */
public final class IPCUtils {

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
  public static final TransportMessage EOI =
      TransportMessage.newBuilder()
          .setType(TransportMessage.Type.DATA)
          .setDataMessage(DataMessage.newBuilder().setType(DataMessage.Type.EOI))
          .build();

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
  public static final TransportMessage CONTROL_SHUTDOWN =
      TransportMessage.newBuilder()
          .setType(TransportMessage.Type.CONTROL)
          .setControlMessage(ControlMessage.newBuilder().setType(ControlMessage.Type.SHUTDOWN))
          .build();

  /** Heartbeat message sent from a worker to tell the master that it is alive. */
  public static final TransportMessage CONTROL_WORKER_HEARTBEAT =
      TransportMessage.newBuilder()
          .setType(TransportMessage.Type.CONTROL)
          .setControlMessage(
              ControlMessage.newBuilder().setType(ControlMessage.Type.WORKER_HEARTBEAT))
          .build();

  /**
   * @param workerId the id of the worker to be removed.
   * @param ackedWorkerIds the ids of all workers who have acked the removed worker.
   * @param workerException the exception associated with the failed worker.
   * @return the remove worker TM.
   * @throws IOException if serializing {@link workerException} fails.
   * */
  public static TransportMessage removeWorkerTM(
      final int workerId,
      @Nullable final Set<Integer> ackedWorkerIds,
      @Nullable final Throwable workerException)
      throws IOException {
    ControlMessage.Builder cmBuilder =
        ControlMessage.newBuilder()
            .setType(ControlMessage.Type.REMOVE_WORKER)
            .setWorkerId(workerId);
    if (ackedWorkerIds != null) {
      cmBuilder.addAllAckedWorkerIds(ackedWorkerIds);
    }
    if (workerException != null) {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ObjectOutput out = new ObjectOutputStream(bos)) {
        out.writeObject(workerException);
        cmBuilder.setWorkerException(
            ControlProto.Exception.newBuilder()
                .setException(ByteString.copyFrom(bos.toByteArray())));
      }
    }
    return TransportMessage.newBuilder()
        .setType(TransportMessage.Type.CONTROL)
        .setControlMessage(cmBuilder.build())
        .build();
  }

  /**
   * @param workerId the id of the worker to be removed.
   * @return the remove worker TM.
   * */
  public static TransportMessage removeWorkerAckTM(final int workerId) {
    ControlMessage.Builder cmBuilder =
        ControlMessage.newBuilder()
            .setType(ControlMessage.Type.REMOVE_WORKER_ACK)
            .setWorkerId(workerId);
    return TransportMessage.newBuilder()
        .setType(TransportMessage.Type.CONTROL)
        .setControlMessage(cmBuilder.build())
        .build();
  }

  /**
   * @param workerId the id of the worker to be added.
   * @param socketinfo the SocketInfo of the worker to be added.
   * @return the add worker TM.
   * */
  public static TransportMessage addWorkerTM(
      final int workerId,
      final SocketInfo socketinfo,
      @Nullable final Set<Integer> ackedWorkerIds) {
    ControlMessage.Builder cmBuilder =
        ControlMessage.newBuilder()
            .setType(ControlMessage.Type.ADD_WORKER)
            .setWorkerId(workerId)
            .setRemoteAddress(socketinfo.toProtobuf());
    if (ackedWorkerIds != null) {
      cmBuilder.addAllAckedWorkerIds(ackedWorkerIds);
    }
    return TransportMessage.newBuilder()
        .setType(TransportMessage.Type.CONTROL)
        .setControlMessage(cmBuilder.build())
        .build();
  }

  /**
   * @param workerId the id of the worker to be added.
   * @return the add worker TM.
   * */
  public static TransportMessage addWorkerAckTM(final int workerId) {
    ControlMessage.Builder cmBuilder =
        ControlMessage.newBuilder()
            .setType(ControlMessage.Type.ADD_WORKER_ACK)
            .setWorkerId(workerId);
    return TransportMessage.newBuilder()
        .setType(TransportMessage.Type.CONTROL)
        .setControlMessage(cmBuilder.build())
        .build();
  }

  /**
   * @param taskId the query/subquery task id.
   * @return a query ready TM.
   * */
  public static TransportMessage queryReadyTM(final SubQueryId taskId) {
    return QUERY_TM_BUILDER
        .get()
        .setQueryMessage(queryMessageOf(taskId, QueryMessage.Type.QUERY_READY_TO_EXECUTE))
        .build();
  }

  /**
   * @param taskId .
   * @param workerId .
   * @return a query recover TM.
   * */
  public static TransportMessage recoverQueryTM(final SubQueryId taskId, final int workerId) {
    return QUERY_TM_BUILDER
        .get()
        .setQueryMessage(
            queryMessageOf(taskId, QueryMessage.Type.QUERY_RECOVER).setWorkerId(workerId))
        .build();
  }

  /**
   * @param taskId the task of the message to be sent
   * @param type the type of the message to be sent
   * @return a builder for a query message
   */
  private static QueryMessage.Builder queryMessageOf(
      final SubQueryId taskId, final QueryMessage.Type type) {
    Objects.requireNonNull(taskId, "taskId");
    Objects.requireNonNull(type, "type");
    return QueryMessage.newBuilder()
        .setQueryId(taskId.getQueryId())
        .setSubqueryId(taskId.getSubqueryId())
        .setType(type);
  }

  /**
   * @param taskId the completed query/subquery task id.
   * @return a query complete TM.
   * @param statistics query execution statistics
   * */
  public static TransportMessage queryCompleteTM(
      final SubQueryId taskId, final ExecutionStatistics statistics) {
    return QUERY_TM_BUILDER
        .get()
        .setQueryMessage(
            queryMessageOf(taskId, QueryMessage.Type.QUERY_COMPLETE)
                .setQueryReport(
                    QueryReport.newBuilder()
                        .setSuccess(true)
                        .setExecutionStatistics(statistics.toProtobuf())))
        .build();
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
   * @param taskId the failed query/subquery task id.
   * @param cause the cause of the failure
   * @param statistics query execution statistics
   * @return a query failed (complete, !success) TM.
   * @throws IOException if any IO error occurs.
   * */
  public static TransportMessage queryFailureTM(
      final SubQueryId taskId, final Throwable cause, final ExecutionStatistics statistics)
      throws IOException {
    final ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(inMemBuffer);
    oos.writeObject(wrapSerializableThrowable(cause));
    oos.flush();
    inMemBuffer.flush();
    return QUERY_TM_BUILDER
        .get()
        .setQueryMessage(
            queryMessageOf(taskId, QueryMessage.Type.QUERY_COMPLETE)
                .setQueryReport(
                    QueryReport.newBuilder()
                        .setSuccess(false)
                        .setExecutionStatistics(statistics.toProtobuf())
                        .setCause(ByteString.copyFrom(inMemBuffer.toByteArray()))))
        .build();
  }

  /**
   * @param taskId the failed query/subquery task id.
   * @return a query failed (complete, !success) TM.
   * */
  public static TransportMessage simpleQueryFailureTM(final SubQueryId taskId) {
    return QUERY_TM_BUILDER
        .get()
        .setQueryMessage(
            queryMessageOf(taskId, QueryMessage.Type.QUERY_COMPLETE)
                .setQueryReport(QueryReport.newBuilder().setSuccess(false)))
        .build();
  }

  /**
   * @param taskId the failed query/subquery task id.
   * @return the query start TM.
   * */
  public static TransportMessage startQueryTM(final SubQueryId taskId) {
    return QUERY_TM_BUILDER
        .get()
        .setQueryMessage(queryMessageOf(taskId, QueryMessage.Type.QUERY_START))
        .build();
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
        ChannelFuture cf = channel.setInterestOps(Channel.OP_READ).awaitUninterruptibly();
        return cf.isSuccess();
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
  public static TransportMessage normalDataMessage(
      final List<? extends Column<?>> dataColumns, final int numTuples) {
    final ColumnMessage[] columnProtos = new ColumnMessage[dataColumns.size()];

    int i = 0;
    for (final Column<?> c : dataColumns) {
      columnProtos[i] = c.serializeToProto();
      i++;
    }
    return DATA_TM_BUILDER
        .get()
        .setDataMessage(
            NORMAL_DATAMESSAGE_BUILDER
                .get()
                .clearColumns()
                .addAllColumns(Arrays.asList(columnProtos))
                .setNumTuples(numTuples))
        .build();
  }

  /**
   * @param dataColumns data columns
   * @param validIndices which tuples are valid in the columns.
   * @return a data TM encoding the data columns.
   * */
  public static TransportMessage normalDataMessage(
      final List<Column<?>> dataColumns, final ImmutableIntArray validIndices) {
    final ColumnMessage[] columnProtos = new ColumnMessage[dataColumns.size()];

    int i = 0;
    for (final Column<?> c : dataColumns) {
      columnProtos[i] = c.serializeToProto(validIndices);
      i++;
    }
    return DATA_TM_BUILDER
        .get()
        .setDataMessage(
            NORMAL_DATAMESSAGE_BUILDER
                .get()
                .clearColumns()
                .addAllColumns(Arrays.asList(columnProtos))
                .setNumTuples(validIndices.length()))
        .build();
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
   * @param taskId the query/subquery task id
   * @param query the query to encode.
   * @throws IOException if error occurs in encoding the query.
   * @return an encoded query TM
   */
  public static TransportMessage queryMessage(final SubQueryId taskId, final SubQueryPlan query)
      throws IOException {
    final ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(inMemBuffer);
    oos.writeObject(query);
    oos.flush();
    inMemBuffer.flush();
    return QUERY_TM_BUILDER
        .get()
        .setQueryMessage(
            queryMessageOf(taskId, QueryMessage.Type.QUERY_DISTRIBUTE)
                .setQuery(
                    QueryProto.Query.newBuilder()
                        .setQuery(ByteString.copyFrom(inMemBuffer.toByteArray()))))
        .build();
  }

  /**
   * @param taskId the query/subquery task to be killed.
   * @return a query ready TM.
   * */
  public static TransportMessage killQueryTM(final SubQueryId taskId) {
    return QUERY_TM_BUILDER
        .get()
        .setQueryMessage(queryMessageOf(taskId, QueryMessage.Type.QUERY_KILL))
        .build();
  }

  /**
   * util classes are not instantiable.
   * */
  private IPCUtils() {}

  /**
   * Resource report message sent to master.
   *
   * @param resourceUsage resource usage.
   * @return the transport message.
   *
   * */
  public static TransportMessage resourceReport(final List<ResourceStats> resourceUsage) {
    ControlMessage.Builder ret =
        ControlMessage.newBuilder().setType(ControlMessage.Type.RESOURCE_STATS);
    for (ResourceStats stats : resourceUsage) {
      ret.addResourceStats(stats.toProtobuf());
    }
    return TransportMessage.newBuilder()
        .setType(TransportMessage.Type.CONTROL)
        .setControlMessage(ret.build())
        .build();
  }
}
