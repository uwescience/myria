package edu.washington.escience.myriad.table;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

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

  int BATCH_SIZE = 100;

  _TupleBatch append(_TupleBatch another);

  _TupleBatch distinct();

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
  boolean getBoolean(int column, int row);

  double getDouble(int column, int row);

  float getFloat(int column, int row);

  int getInt(int column, int row);

  long getLong(int column, int row);

  String getString(int column, int row);

  _TupleBatch groupby();

  int hashCode(int rowIndx);

  Schema inputSchema();

  _TupleBatch intersect(_TupleBatch another);

  _TupleBatch join(_TupleBatch other, Predicate p, _TupleBatch output);

  int numInputTuples();

  int numOutputTuples();

  _TupleBatch orderby();

  List<Column> outputRawData();

  /**
   * The schema of the output tuples. The input schema may change by projects. This method return the final output
   * schema.
   */
  Schema outputSchema();

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
   * @param inputColumnIdx the index of the column to be renamed in the input schema
   * @param newName the new column name
   */
  _TupleBatch renameColumn(int inputColumnIdx, String newName);

  _TupleBatch union(_TupleBatch another);
}
