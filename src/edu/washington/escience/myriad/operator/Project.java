package edu.washington.escience.myriad.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;

/**
 * Project is an operator that implements a relational projection.
 */
public final class Project extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * the child.
   * */
  private Operator child;
  /**
   * The result schema.
   * */
  private final Schema schema;
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
    this.child = child;
    outColumnIndices = fieldList;
    final Schema childSchema = child.getSchema();

    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    for (final int i : fieldList) {
      types.add(childSchema.getColumnType(i));
      names.add(childSchema.getColumnName(i));
    }
    schema = new Schema(types, names);
  }

  @Override
  public void cleanup() {
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    final TupleBatch tmp = child.next();
    if (tmp != null) {
      return tmp.project(outColumnIndices);
    }
    return null;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {

    TupleBatch tb = child.nextReady();
    if (tb != null) {
      return tb.project(outColumnIndices);
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    if (child != children[0]) {
      child = children[0];
    }
  }

}
