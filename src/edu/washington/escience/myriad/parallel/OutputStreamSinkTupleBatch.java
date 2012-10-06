package edu.washington.escience.myriad.parallel;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Predicate.Op;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.table._TupleBatch;

public class OutputStreamSinkTupleBatch implements _TupleBatch {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  private final OutputStream backendStream;
  private final Schema schema;
  private int numInputTuples;

  public OutputStreamSinkTupleBatch(final Schema schema, final OutputStream backend) {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(backend);
    this.backendStream = backend;
    this.schema = schema;
    this.numInputTuples = 0;
  }

  @Override
  public _TupleBatch append(final _TupleBatch another) {
    try {
      this.backendStream.write(new ImmutableInMemoryTupleBatch(this.schema, another.outputRawData(), another
          .numOutputTuples()).toString().getBytes());
      this.backendStream.flush();
      this.numInputTuples += another.numOutputTuples();
    } catch (final IOException e) {
      e.printStackTrace();
    }
    return this;
  }

  @Override
  public _TupleBatch distinct() {
    return this;
  }

  @Override
  public _TupleBatch except(final _TupleBatch another) {
    return this;
  }

  @Override
  public _TupleBatch filter(final int fieldIdx, final Op op, final Object operand) {
    return this;
  }

  @Override
  public boolean getBoolean(final int column, final int row) {
    throw new ArrayIndexOutOfBoundsException();
  }

  @Override
  public double getDouble(final int column, final int row) {
    throw new ArrayIndexOutOfBoundsException();
  }

  @Override
  public float getFloat(final int column, final int row) {
    throw new ArrayIndexOutOfBoundsException();
  }

  @Override
  public int getInt(final int column, final int row) {
    throw new ArrayIndexOutOfBoundsException();
  }

  @Override
  public long getLong(final int column, final int row) {
    throw new ArrayIndexOutOfBoundsException();
  }

  @Override
  public String getString(final int column, final int row) {
    throw new ArrayIndexOutOfBoundsException();
  }

  @Override
  public _TupleBatch groupby() {
    return this;
  }

  @Override
  public int hashCode(final int rowIndx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Schema inputSchema() {
    return this.schema;
  }

  @Override
  public _TupleBatch intersect(final _TupleBatch another) {
    return this;
  }

  @Override
  public _TupleBatch join(final _TupleBatch other, final Predicate p, final _TupleBatch output) {
    return this;
  }

  @Override
  public int numInputTuples() {
    return this.numInputTuples;
  }

  @Override
  public int numOutputTuples() {
    return 0;
  }

  @Override
  public _TupleBatch orderby() {
    return this;
  }

  @Override
  public List<Column> outputRawData() {
    return new ArrayList<Column>(0);
  }

  @Override
  public Schema outputSchema() {
    return this.schema;
  }

  @Override
  public TupleBatchBuffer[] partition(final PartitionFunction<?, ?> p, final TupleBatchBuffer[] buffers) {
    return null;
  }

  @Override
  public _TupleBatch project(final int[] remainingColumns) {
    return this;
  }

  @Override
  public _TupleBatch purgeFilters() {
    return this;
  }

  @Override
  public _TupleBatch purgeProjects() {
    return this;
  }

  @Override
  public _TupleBatch remove(final int innerIdx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch renameColumn(final int inputColumnIdx, final String newName) {
    return this;
  }

  @Override
  public String toString() {
    return this.numInputTuples + " rows";
  }

  @Override
  public _TupleBatch union(final _TupleBatch another) {
    // TODO Auto-generated method stub
    return this;
  }
}
