package edu.washington.escience.myriad.operator;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.ArrayUtils;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Project is an operator that implements a relational projection.
 */
public class Project extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  private Operator child;
  private final Schema td;
  private final Integer[] outFieldIds;

  /**
   * Constructor accepts a child operator to read tuples to apply projection to and a list of fields in output tuple
   * 
   * @param fieldList The ids of the fields child's tupleDesc to project out
   * @param typesList the types of the fields in the final projection
   * @param child The child operator
   * @throws DbException
   */
  public Project(final ArrayList<Integer> fieldList, final ArrayList<Type> typesList, final Operator child)
      throws DbException {
    this(fieldList, typesList.toArray(new Type[] {}), child);
  }

  public Project(final ArrayList<Integer> fieldList, final Type[] types, final Operator child) throws DbException {
    this.child = child;
    outFieldIds = fieldList.toArray(new Integer[] {});
    final String[] fieldAr = new String[fieldList.size()];
    final Schema childtd = child.getSchema();

    for (int i = 0; i < fieldAr.length; i++) {
      fieldAr[i] = childtd.getFieldName(fieldList.get(i));
    }
    td = new Schema(types, fieldAr);
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
  protected _TupleBatch fetchNext() throws NoSuchElementException, DbException {
    _TupleBatch tmp = child.next();
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
    return td;
  }

  @Override
  public void init() throws DbException {
  }

  // @Override
  // public void rewind() throws DbException {
  // child.rewind();
  // }

  @Override
  public void setChildren(final Operator[] children) {
    if (child != children[0]) {
      child = children[0];
    }
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    // TODO Auto-generated method stub
    return null;
  }

}
