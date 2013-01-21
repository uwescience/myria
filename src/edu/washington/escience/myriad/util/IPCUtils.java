package edu.washington.escience.myriad.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage.ControlMessageType;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.QueryProto;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;

/**
 * A bunch of util methods for IPC layer.
 * */
public final class IPCUtils {

  /**
   * util classes are not instantiable.
   * */
  private IPCUtils() {
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

  protected static final ThreadLocal<DataMessage.Builder> EOI_DATAMESSAGE_BUILDER =
      new ThreadLocal<DataMessage.Builder>() {
        @Override
        protected DataMessage.Builder initialValue() {
          return DataMessage.newBuilder().setType(DataMessageType.EOI);
        }
      };

  /**
   * create an EOS data message.
   * */
  public static TransportMessage eosTM(ExchangePairID epID) {
    return DATA_TM_BUILDER.get().setData(EOS_DATAMESSAGE_BUILDER.get().setOperatorID(epID.getLong())).build();
  }

  public static TransportMessage eoiTM(ExchangePairID epID) {
    return DATA_TM_BUILDER.get().setData(EOI_DATAMESSAGE_BUILDER.get().setOperatorID(epID.getLong())).build();
  }

  public static TransportMessage connectTM(Integer myID) {
    return CONTROL_TM_BUILDER.get().setControl(
        ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.CONNECT).setRemoteID(myID).build())
        .build();
  }

  public static Integer checkConnectTM(TransportMessage message) {
    if (message == null) {
      return null;
    }
    if ((message.getType() == TransportMessage.TransportMessageType.CONTROL)
        && (ControlMessage.ControlMessageType.CONNECT == message.getControl().getType())) {
      return message.getControl().getRemoteID();
    }
    return null;
  }

  public static TransportMessage CONTROL_QUERY_READY = TransportMessage.newBuilder().setType(
      TransportMessageType.CONTROL).setControl(
      ControlMessage.newBuilder().setType(ControlMessageType.QUERY_READY_TO_EXECUTE).build()).build();

  public static TransportMessage CONTROL_SHUTDOWN = TransportMessage.newBuilder().setType(TransportMessageType.CONTROL)
      .setControl(ControlMessage.newBuilder().setType(ControlMessageType.SHUTDOWN).build()).build();

  public static TransportMessage CONTROL_START_QUERY = TransportMessage.newBuilder().setType(
      TransportMessageType.CONTROL).setControl(
      ControlMessage.newBuilder().setType(ControlMessageType.START_QUERY).build()).build();

  public static TransportMessage normalDataMessage(List<Column<?>> dataColumns, ExchangePairID operatorPairID) {
    final ColumnMessage[] columnProtos = new ColumnMessage[dataColumns.size()];

    int i = 0;
    for (final Column<?> c : dataColumns) {
      columnProtos[i] = c.serializeToProto();
      i++;
    }
    return DATA_TM_BUILDER.get().setData(
        NORMAL_DATAMESSAGE_BUILDER.get().clearColumns().addAllColumns(Arrays.asList(columnProtos)).setOperatorID(
            operatorPairID.getLong())).build();
  }

  public static TransportMessage[] normalDataMessageMultiCopy(List<Column<?>> dataColumns,
      ExchangePairID[] operatorPairID) {
    final ColumnMessage[] columnProtos = new ColumnMessage[dataColumns.size()];

    int i = 0;
    for (final Column<?> c : dataColumns) {
      columnProtos[i] = c.serializeToProto();
      i++;
    }
    TransportMessage[] result = new TransportMessage[operatorPairID.length];
    DataMessage.Builder dataBuilder = NORMAL_DATAMESSAGE_BUILDER.get();
    dataBuilder.clearColumns().addAllColumns(Arrays.asList(columnProtos));
    for (int j = 0; j < operatorPairID.length; j++) {
      result[j] = DATA_TM_BUILDER.get().setData(dataBuilder.setOperatorID(operatorPairID[j].getLong())).build();
    }
    return result;
  }

  public static TransportMessage queryMessage(Operator[] query) throws IOException {
    ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(inMemBuffer);
    oos.writeObject(query);
    oos.flush();
    inMemBuffer.flush();
    return QUERY_TM_BUILDER.get().setQuery(
        QueryProto.Query.newBuilder().setQuery(ByteString.copyFrom(inMemBuffer.toByteArray()))).build();
  }

  public static TransportMessage CONTROL_DISCONNECT = TransportMessage.newBuilder().setType(
      TransportMessageType.CONTROL).setControl(
      ControlMessage.newBuilder().setType(ControlMessageType.DISCONNECT).build()).build();
}
