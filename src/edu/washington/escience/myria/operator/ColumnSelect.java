package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * {@link ColumnSelect} is an operator that implements column selection on tuples. This is like a relational project,
 * but without duplicate elimination.
 */
public final class ColumnSelect extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * The result schema.
   * */
  private Schema schema;
  /**
   * The column indices to remain.
   * */
  private final int[] outColumnIndices;

  /**
   * @param fieldList The column indices to remain.
   * @param child the child
   * @throws DbException if any error occurs.
   * */
  public ColumnSelect(final int[] fieldList, final Operator child) throws DbException {
    super(child);
    outColumnIndices = fieldList;
    if (child != null) {
      schema = child.getSchema().getSubSchema(outColumnIndices);
    }
  }

  @Override
  public void cleanup() {
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = getChild().nextReady();
    if (tb != null) {
      return tb.selectColumns(outColumnIndices, getSchema());
    }
    return null;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    if (schema == null) {
      schema = generateSchema();
    }
  }

  @Override
  protected Schema generateSchema() {
    final Operator child = getChild();
    if (child == null) {
      return null;
    }
    final Schema childSchema = child.getSchema();
    if (childSchema == null) {
      return null;
    }
    return childSchema.getSubSchema(outColumnIndices);
  }
}
