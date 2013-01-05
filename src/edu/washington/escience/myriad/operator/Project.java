package edu.washington.escience.myriad.operator;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.ArrayUtils;

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

  /**
   * Constructor accepts a child operator to read tuples to apply projection to and a list of fields in output tuple.
   * 
   * @param fieldList The indexes of the fields to project out.
   * @param typesList the types of the fields in the final projection.
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
    final Schema childSchema = child.getSchema();

    for (int i = 0; i < fieldAr.length; i++) {
      fieldAr[i] = childSchema.getFieldName(fieldList.get(i));
    }
    schema = new Schema(types, fieldAr);
  }

  public Project(final Integer[] fieldList, final Operator child) throws DbException {
    this.child = child;
    outFieldIds = fieldList;
    final String[] fieldName = new String[fieldList.length];
    final Type[] fieldType = new Type[fieldList.length];
    final Schema childSchema = child.getSchema();

    for (int i = 0; i < fieldName.length; i++) {
      fieldName[i] = childSchema.getFieldName(fieldList[i]);
      fieldType[i] = childSchema.getFieldType(fieldList[i]);
    }
    schema = new Schema(fieldType, fieldName);
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
