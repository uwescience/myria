package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.LongColumnBuilder;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Counter appends a Column<Long> to the incoming tuples.
 */
public class Counter extends UnaryOperator {
  /**
   * Required for Java serialization.
   */
  private static final long serialVersionUID = 1L;
  /**
   * The name of the counter column.
   */
  private final String columnName;
  /**
   * The counter.
   */
  private long count;

  /**
   * Instantiate a Counter operator using the given column name.
   *
   * @param child the child operator.
   * @param columnName the name of the new column.
   */
  public Counter(final Operator child, final String columnName) {
    super(child);
    this.columnName = columnName;
    /* Generate the Schema now as a way of sanity-checking the constructor arguments. */
    getSchema();
  }

  /**
   * Instantiate a Counter operator with null child. (Must be set later by setChild() or setChildren()).
   *
   * @param columnName the new column name.
   */
  public Counter(final String columnName) {
    this(null, columnName);
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    final TupleBatch childTuples = getChild().nextReady();
    if (childTuples == null) {
      return null;
    }
    LongColumnBuilder builder = new LongColumnBuilder();
    for (int i = 0; i < childTuples.numTuples(); ++i) {
      builder.appendLong(count);
      ++count;
    }
    return childTuples.appendColumn(columnName, builder.build());
  }

  @Override
  public Schema generateSchema() {
    final Operator child = getChild();
    if (child == null) {
      return null;
    }
    final Schema childSchema = child.getSchema();
    if (childSchema == null) {
      return null;
    }
    return Schema.appendColumn(childSchema, Type.LONG_TYPE, columnName);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
    count = 0;
  }
}
