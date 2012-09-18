package edu.washington.escience.myriad.table;

import java.util.List;

import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.parallel.PartitionFunction;

public abstract class TupleBatchAdaptor implements _TupleBatch {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public Schema outputSchema() {
    if (this instanceof TupleBatch) {
      TupleBatch tupleBatch = (TupleBatch) this;
      return tupleBatch.getSchema();
    } else
      throw new UnsupportedOperationException();
  }

  @Override
  public Schema inputSchema() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int numInputTuples() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int numOutputTuples() {
    if (this instanceof TupleBatch) {
      TupleBatch tupleBatch = (TupleBatch) this;
      return tupleBatch.validTupleIndices().length;
    } else
      throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch renameColumn(int inputColumnIdx, String newName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch purgeFilters() {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch project(int[] remainingColumns) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch purgeProjects() {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch append(_TupleBatch another) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch join(_TupleBatch other, Predicate p, _TupleBatch output) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch union(_TupleBatch another) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch intersect(_TupleBatch another) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch except(_TupleBatch another) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch distinct() {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch groupby() {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch orderby() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Column> outputRawData() {
    if (this instanceof TupleBatch) {
      List<Column> output = ColumnFactory.allocateColumns(this.outputSchema());
      TupleBatch tupleBatch = (TupleBatch) this;
      for (int row : tupleBatch.validTupleIndices()) {
        for (int column = 0; column < tupleBatch.numColumns(); ++column) {
          output.get(column).putObject(tupleBatch.getColumn(column).get(row));;
        }
      }
      return output;
    } else
      throw new UnsupportedOperationException();
  }

  @Override
  public TupleBatchBuffer[] partition(PartitionFunction<?, ?> pf, TupleBatchBuffer[] buffers) {
    // p.partition(t, td)
    List<Column> outputData = this.outputRawData();
    Schema s = this.outputSchema();
    int numColumns = outputData.size();

    int[] partitions = pf.partition(outputData, s);

    for (int i = 0; i < partitions.length; i++) {
      int p_of_tuple = partitions[i];
      for (int j = 0; j < numColumns; j++) {
        buffers[p_of_tuple].put(j, outputData.get(j).get(i));
      }
    }
    return buffers;
  }

}
