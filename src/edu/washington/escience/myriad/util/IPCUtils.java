package edu.washington.escience.myriad.util;

import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;

public class IPCUtils {

  /**
   * create an EOS data message
   * */
  public static TransportMessage eosTM(ExchangePairID epID) {
    return TransportMessage.newBuilder().setType(TransportMessageType.DATA).setData(
        DataMessage.newBuilder().setType(DataMessageType.EOS).setOperatorID(epID.getLong())).build();
  }
}
