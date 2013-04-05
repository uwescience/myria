package edu.washington.escience.myriad.operator;

import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableList;

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
  private Operator child;
  private final Schema schema;
  private final int[] outFieldIds; // why not using int[]?

  public Project(final int[] fieldList, final Operator child) throws DbException {
    this.child = child;
    outFieldIds = fieldList;
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
  protected TupleBatch fetchNext() throws NoSuchElementException, DbException {
    final TupleBatch tmp = child.next();
    if (tmp != null) {
      return tmp.project(outFieldIds);
    }
    return null;
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    if (child.nextReady()) {
      return child.next().project(outFieldIds);
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
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    if (child != children[0]) {
      child = children[0];
    }
  }

}
