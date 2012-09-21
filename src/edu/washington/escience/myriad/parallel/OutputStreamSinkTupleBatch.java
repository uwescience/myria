package edu.washington.escience.myriad.parallel;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Predicate.Op;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.table._TupleBatch;

public class OutputStreamSinkTupleBatch implements _TupleBatch {

  private final OutputStream backendStream;
  private final Schema schema;
  private int numInputTuples;

  public OutputStreamSinkTupleBatch(Schema schema, OutputStream backend) {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(backend);
    this.backendStream = backend;
    this.schema = schema;
    this.numInputTuples = 0;
  }

  @Override
  public Schema outputSchema() {
    return this.schema;
  }

  @Override
  public Schema inputSchema() {
    return this.schema;
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
  public _TupleBatch renameColumn(int inputColumnIdx, String newName) {
    return this;
  }

  @Override
  public _TupleBatch filter(int fieldIdx, Op op, Object operand) {
    return this;
  }

  @Override
  public _TupleBatch purgeFilters() {
    return this;
  }

  @Override
  public _TupleBatch project(int[] remainingColumns) {
    return this;
  }

  @Override
  public _TupleBatch purgeProjects() {
    return this;
  }

  @Override
  public _TupleBatch append(_TupleBatch another) {
    try {
      this.backendStream.write(new ImmutableInMemoryTupleBatch(this.schema,another.outputRawData(), another.numOutputTuples()).toString().getBytes());
      this.backendStream.flush();
      this.numInputTuples += another.numOutputTuples();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return this;
  }

  @Override
  public _TupleBatch join(_TupleBatch other, Predicate p, _TupleBatch output) {
    return this;
  }

  @Override
  public _TupleBatch union(_TupleBatch another) {
    // TODO Auto-generated method stub
    return this;
  }

  @Override
  public _TupleBatch intersect(_TupleBatch another) {
    return this;
  }

  @Override
  public _TupleBatch except(_TupleBatch another) {
    return this;
  }

  @Override
  public _TupleBatch distinct() {
    return this;
  }

  @Override
  public _TupleBatch groupby() {
    return this;
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
  public TupleBatchBuffer[] partition(PartitionFunction<?, ?> p, TupleBatchBuffer[] buffers) {
    return null;
  }

  @Override
  public boolean getBoolean(int column, int row) {
    throw new ArrayIndexOutOfBoundsException();
  }

  @Override
  public double getDouble(int column, int row) {
    throw new ArrayIndexOutOfBoundsException();
  }

  @Override
  public float getFloat(int column, int row) {
    throw new ArrayIndexOutOfBoundsException();
  }

  @Override
  public int getInt(int column, int row) {
    throw new ArrayIndexOutOfBoundsException();
  }

  @Override
  public String getString(int column, int row) {
    throw new ArrayIndexOutOfBoundsException();
  }

  public String toString()
  {
    return this.numInputTuples+" rows";
  }

  @Override
  public _TupleBatch remove(int innerIdx) {
    throw new UnsupportedOperationException();
  }
}
