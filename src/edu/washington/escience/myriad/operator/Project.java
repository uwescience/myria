package edu.washington.escience.myriad.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

/**
 * Project is an operator that implements a relational projection.
 */
public final class Project extends UnaryOperator {

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
  public Project(final int[] fieldList, final Operator child) throws DbException {
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
      return tb.project(outColumnIndices);
    }
    return null;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    schema = getChild().getSchema().getSubSchema(outColumnIndices);
  }
}
