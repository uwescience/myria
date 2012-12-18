package edu.washington.escience.myriad.parallel;

import java.util.List;
import java.util.Objects;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

public final class ExchangeData {

  private final ExchangePairID operatorID;
  private final int fromWorkerID;
  private final TupleBatch dataHolder;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param oID the operator to which this TB should be feed
   * @param workerID the source worker where the TB is generated
   * @param
   */
  public ExchangeData(final ExchangePairID oID, final int workerID, final List<Column<?>> columns,
      final Schema inputSchema, final int numTuples) {
    dataHolder = new TupleBatch(inputSchema, columns, numTuples);
    operatorID = oID;
    fromWorkerID = workerID;
    Objects.requireNonNull(columns);
  }

  /**
   * @param oID the operator to which this TB should be feed
   * @param workerID the source worker where the TB is generated
   */
  public ExchangeData(final ExchangePairID oID, final int workerID, final Schema schema) {
    dataHolder = null;
    operatorID = oID;
    fromWorkerID = workerID;
  }

  /**
   * Get the ParallelOperatorID, to which this message is targeted
   */
  public ExchangePairID getOperatorID() {
    return operatorID;
  }

  public TupleBatch getRealData() {
    return dataHolder;
  }

  /**
   * Get the worker id from which the message was sent
   */
  public int getWorkerID() {
    return fromWorkerID;
  }

  public boolean isEos() {
    return dataHolder == null;
  }

  @Override
  public String toString() {
    return "ExchangeData From Worker:" + fromWorkerID + "; to Operator: " + operatorID + ";\nContents:\n" + dataHolder;
  }
}
