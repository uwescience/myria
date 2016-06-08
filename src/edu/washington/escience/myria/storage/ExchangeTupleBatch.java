package edu.washington.escience.myria.storage;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;

/**
 * The extended TupleBatch which holds extra meta information for a batch of tuples received from other workers.
 *
 */
public class ExchangeTupleBatch extends TupleBatch {
  /**
   *
   */
  private static final long serialVersionUID = -2482302493089301646L;

  /**
   * The seq num of the first tuple in this TB.
   * */
  private final long startingSeqNum;

  /**
   * Log seq num. For fault tolerance.
   * */
  private final long lsn;

  /**
   * source worker.
   * */
  private final int sourceWorkerID;

  /**
   * Broken-out copy constructor. Shallow copy of the schema, column list, and the number of tuples; deep copy of the
   * valid tuples since that's what we mutate.
   *
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   * @param numTuples the number of tuples in this TupleBatch.
   * @param startingTupleSeqNum of the tuples in this TB.
   * @param lsn log sequence number, for fault tolerance
   * @param sourceWorkerID which worker the TB from
   * @param isEOI is EOI TB
   */
  private ExchangeTupleBatch(
      final Schema schema,
      final ImmutableList<? extends Column<?>> columns,
      final int numTuples,
      final long startingTupleSeqNum,
      final long lsn,
      final int sourceWorkerID,
      final boolean isEOI) {
    /** For a private copy constructor, no data checks are needed. Checks are only needed in the public constructor. */
    super(schema, columns, numTuples, isEOI);
    startingSeqNum = startingTupleSeqNum;
    this.lsn = lsn;
    this.sourceWorkerID = sourceWorkerID;
  }

  /**
   * wrap a TB in ExchangeTupleBatch.
   *
   * @param data the TB data.
   * @param sourceWorkerID which worker the TB from
   * @return a wrapped ETB
   */
  public static final ExchangeTupleBatch wrap(final TupleBatch data, final int sourceWorkerID) {
    if (data instanceof ExchangeTupleBatch) {
      return (ExchangeTupleBatch) data;
    } else {
      return new ExchangeTupleBatch(
          data.getSchema(),
          data.getDataColumns(),
          data.numTuples(),
          -1,
          -1,
          sourceWorkerID,
          data.isEOI());
    }
  }

  /**
   * @return my LSN.
   * */
  public final long getLSN() {
    return lsn;
  }

  /**
   * @return the sequence number of the first tuple in this TB.
   * */
  public final long getStartingSeqNum() {
    return startingSeqNum;
  }

  /**
   * @return get source worker.
   * */
  public final int getSourceWorkerID() {
    return sourceWorkerID;
  }
}
