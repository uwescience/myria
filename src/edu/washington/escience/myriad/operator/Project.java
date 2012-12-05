package edu.washington.escience.myriad.operator;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.ArrayUtils;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;

/**
 * Project is an operator that implements a relational projection.
 */
public class Project extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  private Operator child;
  private final Schema schema;
  private final Integer[] outFieldIds; // why not using int[]?

  public Project(final List<Integer> fieldList, final Operator child) throws DbException {
    this.child = child;
    outFieldIds = fieldList.toArray(new Integer[] {});
    final Schema childSchema = child.getSchema();

    ImmutableList.Builder<Type> types = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();
    for (int i : fieldList) {
      types.add(childSchema.getFieldType(i));
      names.add(childSchema.getFieldName(i));
    }
    schema = new Schema(types, names);
  }

  public Project(final Integer[] fieldList, final Operator child) throws DbException {
    this(Arrays.asList(fieldList), child);
  }

  @Override
  public void cleanup() {
  }

  /**
   * Operator.fetchNext implementation. Iterates over tuples from the child operator, projecting out the fields from the
   * tuple
   * 
   * @return The next tuple, or null if there are no more tuples
   */
  @Override
  protected TupleBatch fetchNext() throws NoSuchElementException, DbException {
    TupleBatch tmp = child.next();
    if (tmp != null) {
      return tmp.project(ArrayUtils.toPrimitive(outFieldIds));
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

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    if (child.nextReady()) {
      return child.next().project(ArrayUtils.toPrimitive(outFieldIds));
    }
    return null;
  }

}
