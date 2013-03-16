package edu.washington.escience.myriad.parallel;

import java.util.List;
import java.util.Objects;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;

public final class ExchangeData implements ExchangeMessage<TupleBatch> {

  private final ExchangePairID operatorID;
  private final int fromWorkerID;
  private final TupleBatch dataHolder;
  public final long seqNum;

  private boolean eos;
  private boolean eoi;

  public static enum MetaMessage {
    EOS, EOI;
  }

  /**
   * @param oID the operator to which this TB should be feed
   * @param workerID the source worker where the TB is generated
   * @param columns columns.
   * @param inputSchema inputSchema.
   * @param numTuples numTuples.
   */
  public ExchangeData(final ExchangePairID oID, final int workerID, final List<Column<?>> columns,
      final Schema inputSchema, final int numTuples, final long seqNum) {
    Objects.requireNonNull(columns);
    this.seqNum = seqNum;
    dataHolder = new TupleBatch(inputSchema, columns, numTuples, -1, -1);
    operatorID = oID;
    fromWorkerID = workerID;
  }

  /**
   * @param oID the operator to which this TB should be feed
   * @param workerID the source worker where the TB is generated
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
   * Get the ParallelOperatorID, to which this message is targeted
   */
  public ExchangePairID getOperatorID() {
    return operatorID;
  }

  public boolean isEos() {
    return eos;
  }

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
