package edu.washington.escience.myriad.parallel;

import java.util.List;
import java.util.Objects;

import edu.washington.escience.myriad.ExchangeTupleBatch;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;

/**
 * A simple wrapper of a TupleBatch from remote for storing into operator input buffers.
 * */
public final class ExchangeData implements ExchangeMessage<TupleBatch> {

  /**
   * the owner operator of this data.
   * */
  private final ExchangePairID operatorID;
  /**
   * From which worker is this data message.
   * */
  private final int fromWorkerID;
  /**
   * the true data.
   * */
  private final TupleBatch dataHolder;
  /**
   * seq num, for use in fault tolerance.
   * */
  private final long seqNum;

  /**
   * Indicate if it's an EOS message.
   * */
  private boolean eos;

  /**
   * Indicate if it's an EOI message.
   * */
  private boolean eoi;

  /**
   * Non-data message type.
   * */
  public static enum MetaMessage {
    /**
     * end of stream.
     * */
    EOS,
    /**
     * end of iteration.
     * */
    EOI;
  }

  /**
   * @param oID the operator to which this TB should be feed
   * @param workerID the source worker where the TB is generated
   * @param columns columns.
   * @param inputSchema inputSchema.
   * @param numTuples numTuples.
   * @param seqNum for use in fault tolerance.
   */
  public ExchangeData(final ExchangePairID oID, final int workerID, final List<Column<?>> columns,
      final Schema inputSchema, final int numTuples, final long seqNum) {
    Objects.requireNonNull(columns);
    this.seqNum = seqNum;
    dataHolder = new ExchangeTupleBatch(inputSchema, columns, numTuples, -1, seqNum, workerID);
    operatorID = oID;
    fromWorkerID = workerID;
  }

  /**
   * @return my seq num.
   * */
  public long getSeqNum() {
    return seqNum;
  }

  /**
   * Constructing a non-data instance.
   * 
   * @param oID the operator to which this TB should be feed
   * @param workerID the source worker where the TB is generated
   * @param schema the schema of the data message
   * @param msg the MetaMessage type.
   */
  public ExchangeData(final ExchangePairID oID, final int workerID, final Schema schema, final MetaMessage msg) {
    dataHolder = null;
    operatorID = oID;
    fromWorkerID = workerID;
    seqNum = -1;
    eos = (msg == MetaMessage.EOS);
    eoi = (msg == MetaMessage.EOI);
  }

  /**
   * @return the {@link ExchangePairID}, to which this message is targeted.
   */
  public ExchangePairID getOperatorID() {
    return operatorID;
  }

  /**
   * @return if it's an EOS.
   * */
  public boolean isEos() {
    return eos;
  }

  /**
   * @return if it's an EOI.
   * */
  public boolean isEoi() {
    return eoi;
  }

  @Override
  public String toString() {
    if (eos) {
      return "ExchangeData From Worker:" + fromWorkerID + "; to Operator: " + operatorID + ": EOS.";
    }
    if (eoi) {
      return "ExchangeData From Worker:" + fromWorkerID + "; to Operator: " + operatorID + ": EOI.";
    }
    return "ExchangeData From Worker:" + fromWorkerID + "; to Operator: " + operatorID + ";\nContents:\n" + dataHolder;
  }

  /**
   * @return worker id from which the message was sent.
   */
  @Override
  public int getSourceIPCID() {
    return fromWorkerID;
  }

  @Override
  public TupleBatch getData() {
    return dataHolder;
  }
}
