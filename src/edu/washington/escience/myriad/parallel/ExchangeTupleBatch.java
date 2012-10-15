package edu.washington.escience.myriad.parallel;

import java.util.List;
import java.util.Objects;

import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Predicate.Op;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.table._TupleBatch;

public class ExchangeTupleBatch implements _TupleBatch {

  private final ExchangePairID operatorID;
  private final int fromWorkerID;
  private final TupleBatch dataHolder;

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public ExchangeTupleBatch(final ExchangePairID oID, final int workerID, final List<Column> columns,
      final Schema inputSchema, final int numTuples) {
    dataHolder = new TupleBatch(inputSchema, columns, numTuples);
    operatorID = oID;
    fromWorkerID = workerID;
    Objects.requireNonNull(columns);
  }

  // EOS
  public ExchangeTupleBatch(final ExchangePairID oID, final int workerID, final Schema inputSchema) {
    dataHolder = null;
    operatorID = oID;
    fromWorkerID = workerID;
  }

  @Override
  public _TupleBatch append(final _TupleBatch another) {
    return dataHolder.append(another);
  }

  @Override
  public _TupleBatch distinct() {
    return dataHolder.distinct();
  }

  @Override
  public _TupleBatch except(final _TupleBatch another) {
    return dataHolder.except(another);
  }

  @Override
  public _TupleBatch filter(final int fieldIdx, final Op op, final Object operand) {
    return dataHolder.filter(fieldIdx, op, operand);
  }

  @Override
  public boolean getBoolean(final int column, final int row) {
    return dataHolder.getBoolean(column, row);
  }

  @Override
  public double getDouble(final int column, final int row) {
    return dataHolder.getDouble(column, row);
  }

  @Override
  public float getFloat(final int column, final int row) {
    return dataHolder.getFloat(column, row);
  }

  @Override
  public int getInt(final int column, final int row) {
    return dataHolder.getInt(column, row);
  }

  @Override
  public long getLong(final int column, final int row) {
    return dataHolder.getLong(column, row);
  }

  /**
   * Get the ParallelOperatorID, to which this message is targeted
   */
  public ExchangePairID getOperatorID() {
    return operatorID;
  }

  public TupleBatch getRealData() {
    return dataHolder;
  }

  @Override
  public String getString(final int column, final int row) {
    return dataHolder.getString(column, row);
  }

  /**
   * Get the worker id from which the message was sent
   */
  public int getWorkerID() {
    return fromWorkerID;
  }

  @Override
  public _TupleBatch groupby() {
    return dataHolder.groupby();
  }

  @Override
  public int hashCode(final int rowIndx) {
    return dataHolder.hashCode(rowIndx);
  }

  @Override
  public int hashCode4Keys(final int rowIndx, final int[] colIndx) {
    return dataHolder.hashCode4Keys(rowIndx, colIndx);
  }

  @Override
  public Schema inputSchema() {
    // return this.dataHolder.inputSchema();
    return dataHolder.inputSchema();
  }

  @Override
  public _TupleBatch intersect(final _TupleBatch another) {
    return dataHolder.intersect(another);
  }

  public boolean isEos() {
    return dataHolder == null;
  }

  @Override
  public _TupleBatch join(final _TupleBatch other, final Predicate p, final _TupleBatch output) {
    return dataHolder.join(other, p, output);
  }

  @Override
  public int numInputTuples() {
    // return this.dataHolder.numInputTuples();
    return dataHolder.numInputTuples();
  }

  @Override
  public int numOutputTuples() {
    // return this.dataHolder.numOutputTuples();
    return dataHolder.numOutputTuples();
  }

  @Override
  public _TupleBatch orderby() {
    return dataHolder.orderby();
  }

  @Override
  public List<Column> outputRawData() {
    return dataHolder.outputRawData();
  }

  @Override
  public Schema outputSchema() {
    // return this.dataHolder.outputSchema();
    return dataHolder.outputSchema();
  }

  @Override
  public TupleBatchBuffer[] partition(final PartitionFunction<?, ?> p, final TupleBatchBuffer[] buffers) {
    return dataHolder.partition(p, buffers);
  }

  @Override
  public _TupleBatch project(final int[] remainingColumns) {
    return dataHolder.project(remainingColumns);
  }

  @Override
  public _TupleBatch purgeFilters() {
    return dataHolder.purgeFilters();
  }

  @Override
  public _TupleBatch purgeProjects() {
    return dataHolder.purgeProjects();
  }

  @Override
  public _TupleBatch remove(final int innerIdx) {
    return dataHolder.remove(innerIdx);
  }

  @Override
  public _TupleBatch renameColumn(final int inputColumnIdx, final String newName) {
    return dataHolder.renameColumn(inputColumnIdx, newName);
  }

  @Override
  public _TupleBatch union(final _TupleBatch another) {
    return dataHolder.union(another);
  }

}
