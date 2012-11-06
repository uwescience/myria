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
  public _TupleBatch append(final _TupleBatch another) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch distinct() {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch except(final _TupleBatch another) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch groupby() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode(final int rowIndx) {
    if (this instanceof TupleBatch) {
      final TupleBatch tupleBatch = (TupleBatch) this;
      return tupleBatch.hashCode(rowIndx);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public int hashCode(final int rowIndx, final int[] colIndx) {
    if (this instanceof TupleBatch) {
      final TupleBatch tupleBatch = (TupleBatch) this;
      return tupleBatch.hashCode(rowIndx, colIndx);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Schema inputSchema() {
    if (this instanceof TupleBatch) {
      final TupleBatch tupleBatch = (TupleBatch) this;
      return tupleBatch.getSchema();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public _TupleBatch intersect(final _TupleBatch another) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch join(final _TupleBatch other, final Predicate p, final _TupleBatch output) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int numInputTuples() {
    if (this instanceof TupleBatch) {
      final TupleBatch tupleBatch = (TupleBatch) this;
      return tupleBatch.getNumTuples();
      // return tupleBatch.validTupleIndices().length; // need a public method here
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public int numOutputTuples() {
    if (this instanceof TupleBatch) {
      final TupleBatch tupleBatch = (TupleBatch) this;
      return tupleBatch.validTupleIndices().length;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public _TupleBatch orderby() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Column> outputRawData() {
    if (this instanceof TupleBatch) {
      final List<Column> output = ColumnFactory.allocateColumns(outputSchema());
      final TupleBatch tupleBatch = (TupleBatch) this;
      for (final int row : tupleBatch.validTupleIndices()) {
        for (int column = 0; column < tupleBatch.numColumns(); ++column) {
          output.get(column).putObject(tupleBatch.getColumn(column).get(row));
        }
      }
      return output;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Schema outputSchema() {
    if (this instanceof TupleBatch) {
      final TupleBatch tupleBatch = (TupleBatch) this;
      return tupleBatch.getSchema();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public TupleBatchBuffer[] partition(final PartitionFunction<?, ?> pf, final TupleBatchBuffer[] buffers) {
    // p.partition(t, td)
    final List<Column> outputData = outputRawData();
    final Schema s = outputSchema();
    final int numColumns = outputData.size();

    final int[] partitions = pf.partition(outputData, s);

    for (int i = 0; i < partitions.length; i++) {
      final int p_of_tuple = partitions[i];
      for (int j = 0; j < numColumns; j++) {
        buffers[p_of_tuple].put(j, outputData.get(j).get(i));
      }
    }
    return buffers;
  }

  @Override
  public _TupleBatch project(final int[] remainingColumns) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch purgeFilters() {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch purgeProjects() {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch renameColumn(final int inputColumnIdx, final String newName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch union(final _TupleBatch another) {
    throw new UnsupportedOperationException();
  }

}
