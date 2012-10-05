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
 * Relational data processing units
 */
public interface _TupleBatch extends Serializable {

  public interface TupleIterator extends Iterator {
  }

  public static final int BATCH_SIZE = 100;

  public _TupleBatch append(_TupleBatch another);

  public _TupleBatch distinct();

  public _TupleBatch except(_TupleBatch another);

  /**
   * @param fieldIdx the index of input columns
   * 
   *          select only those tuples which fulfill the predicate. The effects of multiple select operations overlap.
   * 
   */
  public _TupleBatch filter(int fieldIdx, Predicate.Op op, Object operand);

  /**
   * -------------------The data processing methods --------------------
   */

  /**
   * -------------------- The value retrieval methods ------------------
   */
  public boolean getBoolean(int column, int row);

  public double getDouble(int column, int row);

  public float getFloat(int column, int row);

  public int getInt(int column, int row);

  public long getLong(int column, int row);

  public String getString(int column, int row);

  public _TupleBatch groupby();

  public int hashCode(int rowIndx);

  public Schema inputSchema();

  public _TupleBatch intersect(_TupleBatch another);

  public _TupleBatch join(_TupleBatch other, Predicate p, _TupleBatch output);

  public int numInputTuples();

  public int numOutputTuples();

  public _TupleBatch orderby();

  public List<Column> outputRawData();

  /**
   * The schema of the output tuples. The input schema may change by projects. This method return the final output
   * schema.
   */
  public Schema outputSchema();

  /**
   * -------------------- The parallel methods ------------------------
   */

  public TupleBatchBuffer[] partition(PartitionFunction<?, ?> p, TupleBatchBuffer[] buffers);

  /**
   * @param remainingColumns the indices of the input columns
   * 
   *          multiple calls to this method
   */
  public _TupleBatch project(int[] remainingColumns);

  /**
   * Clear all the filters
   */
  public _TupleBatch purgeFilters();

  /**
   * Clear all the projects
   */
  public _TupleBatch purgeProjects();

  public _TupleBatch remove(int innerIdx);

  /**
   * @param inputColumnIdx the index of the column to be renamed in the input schema
   * @param newName the new column name
   */
  public _TupleBatch renameColumn(int inputColumnIdx, String newName);

  public _TupleBatch union(_TupleBatch another);
}
