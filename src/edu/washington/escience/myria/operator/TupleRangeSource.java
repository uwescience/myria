package edu.washington.escience.myria.operator;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * This class generates a range of single-column tuples.
 *
 */
public final class TupleRangeSource extends LeafOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The tuples that this operator serves, exactly once. */
  private long currentValue;
  /** The number of tuples that have been generated so far. */
  private long currentCount;
  /** The number of tuples to generate in total. */
  private final long count;
  /** The Schema of the tuples that this operator serves. */
  private final Schema schema;
  /** The type of the tuples to be emitted. */
  private final Type type;

  /**
   * Construct the TupleRangeSource.
   *
   * @param count the total number of tuples to output.
   * @param type the type of those tuples.
   */
  public TupleRangeSource(final long count, final Type type) {
    this.count = count;
    this.type = type;
    schema = Schema.of(ImmutableList.of(type), ImmutableList.of("value"));
    currentCount = 0L;
    currentValue = 0L;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    if (currentCount == count) {
      return null;
    }
    TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < TupleBatch.BATCH_SIZE && currentCount < count; ++i) {
      switch (type) {
        case BOOLEAN_TYPE:
          tbb.putBoolean(0, currentValue % 2 == 0);
          break;
        case DATETIME_TYPE:
          tbb.putDateTime(0, new DateTime(currentValue));
          break;
        case DOUBLE_TYPE:
          tbb.putDouble(0, currentValue);
          break;
        case FLOAT_TYPE:
          tbb.putFloat(0, currentValue);
          break;
        case INT_TYPE:
          tbb.putInt(0, (int) currentValue);
          break;
        case LONG_TYPE:
          tbb.putLong(0, currentValue);
          break;
        case STRING_TYPE:
          tbb.putString(0, String.valueOf(currentValue));
          break;
      }
      currentValue++;
      currentCount++;
    }
    return tbb.popAny();
  }

  @Override
  protected Schema generateSchema() {
    return schema;
  }
}
