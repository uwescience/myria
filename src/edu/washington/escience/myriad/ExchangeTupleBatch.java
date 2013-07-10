package edu.washington.escience.myriad;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.util.ImmutableBitSet;

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
   * @param validTuples BitSet determines which tuples are valid tuples in this batch.
   * @param validIndices valid tuple indices.
   * @param startingTupleSeqNum of the tuples in this TB.
   * @param lsn log sequence number, for fault tolerance
   * @param sourceWorkerID which worker the TB from
   */
  private ExchangeTupleBatch(final Schema schema, final ImmutableList<Column<?>> columns,
      final ImmutableBitSet validTuples, final int[] validIndices, final long startingTupleSeqNum, final long lsn,
      final int sourceWorkerID, final boolean isEOI) {
    /** For a private copy constructor, no data checks are needed. Checks are only needed in the public constructor. */
    super(schema, columns, validTuples, validIndices, isEOI);
    startingSeqNum = startingTupleSeqNum;
    this.lsn = lsn;
    this.sourceWorkerID = sourceWorkerID;
  }

  /**
   * Standard immutable TupleBatch constructor. All fields must be populated before creation and cannot be changed.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   * @param numTuples number of tuples in the batch.
   * @param startingTupleSeqNum of the tuples in this TB.
   * @param lsn log sequence number, for fault tolerance
   * @param sourceWorkerID which worker the TB from
   */
  public ExchangeTupleBatch(final Schema schema, final List<Column<?>> columns, final int numTuples,
      final long startingTupleSeqNum, final long lsn, final int sourceWorkerID) {
    /* Take the input arguments directly */
    super(schema, columns, numTuples);
    startingSeqNum = startingTupleSeqNum;
    this.lsn = lsn;
    this.sourceWorkerID = sourceWorkerID;
  }

  /**
   * Standard immutable TupleBatch constructor. All fields must be populated before creation and cannot be changed.
   * 
   * @param data the TB data.
   * @param sourceWorkerID which worker the TB from
   */
  public ExchangeTupleBatch(final TupleBatch data, final int sourceWorkerID) {
    /* Take the input arguments directly */
    super(data.getSchema(), data.getDataColumns(), data.getValidTuples(), data.getValidIndices(), data.isEOI());
    this.sourceWorkerID = sourceWorkerID;
    lsn = -1;
    startingSeqNum = -1;
  }

  @Override
  protected final TupleBatch shallowCopy(final Schema schema, final ImmutableList<Column<?>> columns,
      final ImmutableBitSet validTuples, final int[] validIndices, final boolean isEOI) {
    return new ExchangeTupleBatch(schema, columns, validTuples, validIndices, startingSeqNum, lsn, sourceWorkerID,
        isEOI);
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
