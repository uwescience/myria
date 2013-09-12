package edu.washington.escience.myria.operator;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;

/**
 * Rename operator is a UnaryOperator that makes a shallow modification to child TupleBatches: it merely changes their
 * column names.
 * 
 * @author dhalperi
 * 
 */
public class Rename extends UnaryOperator {
  /**
   * Required for Java serialization.
   */
  private static final long serialVersionUID = 1L;
  /**
   * The new names of the columns in the tuples emitted by this Operator.
   */
  private final ImmutableList<String> columnNames;
  /**
   * The schema of the tuples emitted by this Operator.
   */
  private Schema schema;

  /**
   * Instantiate a rename operator, which renames the columns of its child.
   * 
   * @param child the child operator.
   * @param columnNames the new column names.
   */
  public Rename(final Operator child, final List<String> columnNames) {
    super(child);
    this.columnNames = ImmutableList.copyOf(columnNames);
    if (child != null && child.getSchema() != null) {
      schema = new Schema(child.getSchema().getColumnTypes(), columnNames);
    }
  }

  /**
   * Instantiate a rename operator with null child. (Must be set later by setChild() or setChildren()).
   * 
   * @param columnNames the new column names.
   */
  public Rename(final List<String> columnNames) {
    this(null, columnNames);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
    if (schema == null) {
      schema = new Schema(getChild().getSchema().getColumnTypes(), columnNames);
    }
  }

  @Override
  protected void cleanup() throws Exception {
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    final TupleBatch childTuples = getChild().fetchNextReady();
    if (childTuples == null) {
      return null;
    }
    return childTuples.rename(columnNames);
  }

  @Override
  public Schema getSchema() {
    if (schema == null && getChild() != null && getChild().getSchema() != null) {
      schema = new Schema(getChild().getSchema().getColumnTypes(), columnNames);
    }
    return schema;
  }

}
