package edu.washington.escience.myriad.util;

import java.util.Arrays;
import java.util.List;

import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
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
  protected static final ThreadLocal<TransportMessage.Builder> TRANSPORTMESSAGE_BUILDER =
      new ThreadLocal<TransportMessage.Builder>() {
        @Override
        protected TransportMessage.Builder initialValue() {
          return TransportMessage.newBuilder().setType(TransportMessageType.DATA);
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
   * create an EOS data message
   * */
  public static TransportMessage eosTM(ExchangePairID epID) {
    return TRANSPORTMESSAGE_BUILDER.get().setData(EOS_DATAMESSAGE_BUILDER.get().setOperatorID(epID.getLong())).build();
  }

  public static TransportMessage connectTM(Integer myID) {
    return TransportMessage.newBuilder().setType(TransportMessageType.CONTROL).setControl(
        ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.CONNECT).setRemoteID(myID).build())
        .build();
  }

  public static TransportMessage normalDataMessage(List<Column<?>> dataColumns, ExchangePairID operatorPairID) {
    final ColumnMessage[] columnProtos = new ColumnMessage[dataColumns.size()];

    int i = 0;
    for (final Column<?> c : dataColumns) {
      columnProtos[i] = c.serializeToProto();
      i++;
    }
    return TRANSPORTMESSAGE_BUILDER.get().setData(
        NORMAL_DATAMESSAGE_BUILDER.get().clearColumns().addAllColumns(Arrays.asList(columnProtos)).setOperatorID(
            operatorPairID.getLong())).build();
  }
}
