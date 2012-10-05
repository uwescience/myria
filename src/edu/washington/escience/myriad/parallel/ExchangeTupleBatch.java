package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import edu.washington.escience.myriad.column.BooleanColumn;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.column.FloatColumn;
import edu.washington.escience.myriad.column.IntColumn;
import edu.washington.escience.myriad.column.LongColumn;
import edu.washington.escience.myriad.column.StringColumn;
import edu.washington.escience.myriad.parallel.ImmutableInMemoryTupleBatch;
import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Predicate.Op;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.table._TupleBatch;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage;

public class ExchangeTupleBatch extends ExchangeMessage implements _TupleBatch {

  // private final ImmutableInMemoryTupleBatch dataHolder;
  ColumnMessage[] data;
  int numTuples;
  Schema inputSchema;

  public ExchangeTupleBatch(ExchangePairID oID, String workerID, List<Column> columns, Schema inputSchema, int numTuples) {
    super(oID, workerID);
    Objects.requireNonNull(columns);
    this.data = new ColumnMessage[columns.size()];
    {
      int i = 0;
      for (Column c : columns) {
        this.data[i] = c.serializeToProto();
        i++;
      }
    }

    this.numTuples = numTuples;
    this.inputSchema = inputSchema;
    // dataHolder = new ImmutableInMemoryTupleBatch(inputSchema, columns, numTuples);
  }

  // EOS
  public ExchangeTupleBatch(ExchangePairID oID, String workerID) {
    super(oID, workerID);
    this.data = null;
  }

  public boolean isEos() {
    return this.data == null;
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public Schema outputSchema() {
    // return this.dataHolder.outputSchema();
    return this.inputSchema;
  }

  @Override
  public Schema inputSchema() {
    // return this.dataHolder.inputSchema();
    return this.inputSchema;
  }

  @Override
  public int numInputTuples() {
    // return this.dataHolder.numInputTuples();
    return this.numTuples;
  }

  @Override
  public int numOutputTuples() {
    // return this.dataHolder.numOutputTuples();
    return this.numTuples;
  }

  @Override
  public _TupleBatch renameColumn(int inputColumnIdx, String newName) {
    // return this.dataHolder.renameColumn(inputColumnIdx, newName);
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch filter(int fieldIdx, Op op, Object operand) {
    // return this.dataHolder.filter(fieldIdx, op, operand);
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch purgeFilters() {
    // return this.dataHolder.purgeFilters();
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch project(int[] remainingColumns) {
    // return this.dataHolder.project(remainingColumns);
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch purgeProjects() {
    // return this.dataHolder.purgeProjects();
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch append(_TupleBatch another) {
    // return this.dataHolder.append(another);
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch join(_TupleBatch other, Predicate p, _TupleBatch output) {
    // return this.dataHolder.join(other, p, output);
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch union(_TupleBatch another) {
    // return this.dataHolder.union(another);
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch intersect(_TupleBatch another) {
    // return this.dataHolder.intersect(another);
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch except(_TupleBatch another) {
    // return this.dataHolder.except(another);
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch distinct() {
    // return this.dataHolder.distinct();
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch groupby() {
    // return this.dataHolder.groupby();
    throw new UnsupportedOperationException();
  }

  @Override
  public _TupleBatch orderby() {
    // return this.dataHolder.orderby();
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBoolean(int column, int row) {
    // return this.dataHolder.getBoolean(column, row);
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int column, int row) {
    // return this.dataHolder.getDouble(column, row);
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int column, int row) {
    // return this.dataHolder.getFloat(column, row);
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int column, int row) {
    // return this.dataHolder.getInt(column, row);
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(int column, int row) {
    // return this.dataHolder.getString(column, row);
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Column> outputRawData() {
    // return this.dataHolder.outputRawData();
    // return this.data
    if (this.data == null)
      return null;

    List<Column> output = new ArrayList<Column>(this.data.length);
    Type[] types = this.inputSchema.getTypes();
    {
      int i = 0;
      for (ColumnMessage cm : this.data) {
        if (types[i].equals(Type.BOOLEAN_TYPE))
          output.add(new BooleanColumn(cm));
        else if (types[i].equals(Type.DOUBLE_TYPE))
          output.add(new DoubleColumn(cm));
        else if (types[i].equals(Type.FLOAT_TYPE))
          output.add(new FloatColumn(cm));
        else if (types[i].equals(Type.LONG_TYPE))
          output.add(new LongColumn(cm));
        else if (types[i].equals(Type.INT_TYPE))
          output.add(new IntColumn(cm));
        else if (types[i].equals(Type.STRING_TYPE))
          output.add(new StringColumn(cm));
        i++;
      }
    }
    return output;
  }

  @Override
  public _TupleBatch[] partition(PartitionFunction<?, ?> p, _TupleBatch[] buffers) {
    // TODO Auto-generated method stub
    return null;
  }

}
