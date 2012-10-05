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

public class ExchangeTupleBatch implements ExchangeMessage, _TupleBatch {

  private final ExchangePairID operatorID;
  private final int fromWorkerID;
  private final TupleBatch dataHolder;

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public ExchangeTupleBatch(final ExchangePairID oID, final int workerID, final List<Column> columns, final Schema inputSchema, final int numTuples) {
    this.dataHolder = new TupleBatch(inputSchema, columns, numTuples);
    this.operatorID = oID;
    this.fromWorkerID = workerID;
    Objects.requireNonNull(columns);
  }

  // EOS
  public ExchangeTupleBatch(final ExchangePairID oID, final int workerID, final Schema inputSchema) {
    this.dataHolder = null;
    this.operatorID = oID;
    this.fromWorkerID = workerID;
  }

  @Override
  public _TupleBatch append(final _TupleBatch another) {
    return this.dataHolder.append(another);
  }

  @Override
  public _TupleBatch distinct() {
    return this.dataHolder.distinct();
  }

  @Override
  public _TupleBatch except(final _TupleBatch another) {
    return this.dataHolder.except(another);
  }

  @Override
  public _TupleBatch filter(final int fieldIdx, final Op op, final Object operand) {
    return this.dataHolder.filter(fieldIdx, op, operand);
  }

  @Override
  public boolean getBoolean(final int column, final int row) {
    return this.dataHolder.getBoolean(column, row);
  }

  @Override
  public double getDouble(final int column, final int row) {
    return this.dataHolder.getDouble(column, row);
  }

  @Override
  public float getFloat(final int column, final int row) {
    return this.dataHolder.getFloat(column, row);
  }

  @Override
  public int getInt(final int column, final int row) {
    return this.dataHolder.getInt(column, row);
  }

  @Override
  public long getLong(final int column, final int row) {
    return this.dataHolder.getLong(column, row);
  }

  /**
   * Get the ParallelOperatorID, to which this message is targeted
   * */
  @Override
  public ExchangePairID getOperatorID() {
    return this.operatorID;
  }

  public TupleBatch getRealData() {
    return dataHolder;
  }

  @Override
  public String getString(final int column, final int row) {
    return this.dataHolder.getString(column, row);
  }

  /**
   * Get the worker id from which the message was sent
   * */
  @Override
  public int getWorkerID() {
    return this.fromWorkerID;
  }

  @Override
  public _TupleBatch groupby() {
    return this.dataHolder.groupby();
  }

  @Override
  public int hashCode(final int rowIndx) {
    return this.dataHolder.hashCode(rowIndx);
  }

  @Override
  public Schema inputSchema() {
    // return this.dataHolder.inputSchema();
    return this.dataHolder.inputSchema();
  }

  @Override
  public _TupleBatch intersect(final _TupleBatch another) {
    return this.dataHolder.intersect(another);
  }

  public boolean isEos() {
    return this.dataHolder == null;
  }

  @Override
  public _TupleBatch join(final _TupleBatch other, final Predicate p, final _TupleBatch output) {
    return this.dataHolder.join(other, p, output);
  }

  @Override
  public int numInputTuples() {
    // return this.dataHolder.numInputTuples();
    return this.dataHolder.numInputTuples();
  }

  @Override
  public int numOutputTuples() {
    // return this.dataHolder.numOutputTuples();
    return this.dataHolder.numOutputTuples();
  }

  @Override
  public _TupleBatch orderby() {
    return this.dataHolder.orderby();
  }

  @Override
  public List<Column> outputRawData() {
    return this.dataHolder.outputRawData();
  }

  @Override
  public Schema outputSchema() {
    // return this.dataHolder.outputSchema();
    return this.dataHolder.outputSchema();
  }

  @Override
  public TupleBatchBuffer[] partition(final PartitionFunction<?, ?> p, final TupleBatchBuffer[] buffers) {
    return this.dataHolder.partition(p, buffers);
  }

  @Override
  public _TupleBatch project(final int[] remainingColumns) {
    return this.dataHolder.project(remainingColumns);
  }

  @Override
  public _TupleBatch purgeFilters() {
    return this.dataHolder.purgeFilters();
  }

  @Override
  public _TupleBatch purgeProjects() {
    return this.dataHolder.purgeProjects();
  }

  @Override
  public _TupleBatch remove(final int innerIdx) {
    return this.dataHolder.remove(innerIdx);
  }

  @Override
  public _TupleBatch renameColumn(final int inputColumnIdx, final String newName) {
    return this.dataHolder.renameColumn(inputColumnIdx, newName);
  }

  @Override
  public _TupleBatch union(final _TupleBatch another) {
    return this.dataHolder.union(another);
  }

}
