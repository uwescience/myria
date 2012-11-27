package edu.washington.escience.myriad.table;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.parallel.PartitionFunction;

/**
 * Relational data processing units.
 */
public interface _TupleBatch extends Serializable {

  public interface TupleIterator extends Iterator<_TupleBatch> {
  }

  /**
   * Appends the contents of another _TupleBatch to this object. Mutates this object.
   * 
   * @param another the source of tuples.
   * @return this _TupleBatch, for use in method chaining.
   */
  _TupleBatch append(_TupleBatch another);

  /**
   * Unimplemented helper method. TODO
   * 
   * @return this _TupleBatch, for use in method chaining.
   */
  _TupleBatch distinct();

  /**
   * Unimplemented difference operator. TODO
   * 
   * @param another the source of tuples.
   * @return this _TupleBatch, for use in method chaining.
   */
  _TupleBatch except(_TupleBatch another);

  /**
   * @param fieldIdx the index of input columns
   * 
   *          select only those tuples which fulfill the predicate. The effects of multiple select operations overlap.
   * 
   */
  _TupleBatch filter(int fieldIdx, Predicate.Op op, Object operand);

  /* -------------------The data processing methods -------------------- */

  /* -------------------- The value retrieval methods ------------------ */
  /**
   * Returns the element at the specified column and row position.
   * 
   * @param column column in which the element is stored.
   * @param row row in which the element is stored.
   * @return the element at the specified position in this TupleBatch.
   */
  boolean getBoolean(int column, int row);

  /**
   * Returns the element at the specified column and row position.
   * 
   * @param column column in which the element is stored.
   * @param row row in which the element is stored.
   * @return the element at the specified position in this TupleBatch.
   */
  double getDouble(int column, int row);

  /**
   * Returns the element at the specified column and row position.
   * 
   * @param column column in which the element is stored.
   * @param row row in which the element is stored.
   * @return the element at the specified position in this TupleBatch.
   */
  float getFloat(int column, int row);

  /**
   * Returns the element at the specified column and row position.
   * 
   * @param column column in which the element is stored.
   * @param row row in which the element is stored.
   * @return the element at the specified position in this TupleBatch.
   */
  int getInt(int column, int row);

  /**
   * Returns the element at the specified column and row position.
   * 
   * @param column column in which the element is stored.
   * @param row row in which the element is stored.
   * @return the element at the specified position in this TupleBatch.
   */
  long getLong(int column, int row);

  /**
   * Returns the element at the specified column and row position.
   * 
   * @param column column in which the element is stored.
   * @param row row in which the element is stored.
   * @return the element at the specified position in this TupleBatch.
   */
  Object getObject(int column, int row);

  /**
   * Returns the element at the specified column and row position.
   * 
   * @param column column in which the element is stored.
   * @param row row in which the element is stored.
   * @return the element at the specified position in this TupleBatch.
   */
  String getString(int column, int row);

  /**
   * Returns the hash of the Objects in the specified row.
   * 
   * @param rowIndx the row to hash.
   * @return the hash value of the Objects in the specified row.
   */
  int hashCode(final int rowIndx);

  /**
   * Returns the hash of the Objects in the specified row and columns.
   * 
   * @param index the row to hash.
   * @param colIndx the columns to include.
   * @return the hash value of the Objects in the specified row and columns.
   */
  int hashCode(final int index, final int[] colIndx);

  /**
   * @return the Schema of the tuples input to this _TupleBatch.
   */
  Schema inputSchema();

  int numInputTuples();

  int numOutputTuples();

  List<Column> outputRawData();

  /**
   * @return the schema of the output tuples. The schema may change by projects. This method return the final output
   *         schema.
   */
  Schema outputSchema();

  Set<Pair<Object, TupleBatchBuffer>> groupby(int groupByColumn, Map<Object, Pair<Object, TupleBatchBuffer>> buffers);

  /* -------------------- The parallel methods ------------------------ */

  TupleBatchBuffer[] partition(PartitionFunction<?, ?> p, TupleBatchBuffer[] buffers);

  /**
   * @param remainingColumns the indices of the input columns
   * 
   *          multiple calls to this method
   */
  _TupleBatch project(int[] remainingColumns);

  /**
   * Clear all the filters.
   */
  _TupleBatch purgeFilters();

  /**
   * Clear all the projects.
   */
  _TupleBatch purgeProjects();

  _TupleBatch remove(int innerIdx);

  /**
   * @param inputColumnIdx the index of the column to be renamed in the input schema.
   * @param newName the new column name.
   */
  _TupleBatch renameColumn(int inputColumnIdx, String newName);

  _TupleBatch union(_TupleBatch another);

}
