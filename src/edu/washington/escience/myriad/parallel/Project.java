package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.ArrayUtils;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;
import edu.washington.escience.myriad.Type;

/**
 * Project is an operator that implements a relational projection.
 */
public class Project extends Operator {

  private Operator child;
  private final Schema td;
  private final Integer[] outFieldIds;

  /**
   * Constructor accepts a child operator to read tuples to apply projection to and a list of fields in output tuple
   * 
   * @param fieldList The ids of the fields child's tupleDesc to project out
   * @param typesList the types of the fields in the final projection
   * @param child The child operator
   */
  public Project(ArrayList<Integer> fieldList, ArrayList<Type> typesList, Operator child) {
    this(fieldList, typesList.toArray(new Type[] {}), child);
  }

  public Project(ArrayList<Integer> fieldList, Type[] types, Operator child) {
    this.child = child;
    outFieldIds = fieldList.toArray(new Integer[] {});
    String[] fieldAr = new String[fieldList.size()];
    Schema childtd = child.getSchema();

    for (int i = 0; i < fieldAr.length; i++) {
      fieldAr[i] = childtd.getFieldName(fieldList.get(i));
    }
    td = new Schema(types, fieldAr);
  }

  @Override
  public void close() {
    super.close();
    child.close();
  }

  /**
   * Operator.fetchNext implementation. Iterates over tuples from the child operator, projecting out the fields from the
   * tuple
   * 
   * @return The next tuple, or null if there are no more tuples
   */
  @Override
  protected _TupleBatch fetchNext() throws NoSuchElementException, DbException {
    if (child.hasNext()) {
      return child.next().project(ArrayUtils.toPrimitive(this.outFieldIds));
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { this.child };
  }

  @Override
  public Schema getSchema() {
    return td;
  }

  @Override
  public void open() throws DbException, NoSuchElementException {
    child.open();
    super.open();
  }

  // @Override
  // public void rewind() throws DbException {
  // child.rewind();
  // }

  @Override
  public void setChildren(Operator[] children) {
    if (this.child != children[0]) {
      this.child = children[0];
    }
  }

}
