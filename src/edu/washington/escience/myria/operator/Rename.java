package edu.washington.escience.myria.operator;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

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
   * Instantiate a rename operator, which renames the columns of its child.
   * 
   * @param child the child operator.
   * @param columnNames the new column names.
   */
  public Rename(final Operator child, final List<String> columnNames) {
    super(child);
    this.columnNames = ImmutableList.copyOf(columnNames);
    /* Generate the Schema now as a way of sanity-checking the constructor arguments. */
    getSchema();
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
  protected TupleBatch fetchNextReady() throws Exception {
    final TupleBatch childTuples = getChild().fetchNextReady();
    if (childTuples == null) {
      return null;
    }
    return childTuples.rename(columnNames);
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
    if (columnNames == null) {
      return null;
    }
    return new Schema(childSchema.getColumnTypes(), columnNames);
  }

}
