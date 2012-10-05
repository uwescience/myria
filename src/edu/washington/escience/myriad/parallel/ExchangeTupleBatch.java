package edu.washington.escience.myriad.parallel;

import java.util.List;
import java.util.Objects;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Predicate.Op;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.table._TupleBatch;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

public class ExchangeTupleBatch implements ExchangeMessage, _TupleBatch {

  private final ExchangePairID operatorID;
  private final int fromWorkerID;
  private final TupleBatch dataHolder;
  
  
  public TupleBatch getRealData()
  {
    return dataHolder;
  }

  public ExchangeTupleBatch(ExchangePairID oID, int workerID, List<Column> columns, Schema inputSchema, int numTuples) {
    this.dataHolder = new TupleBatch(inputSchema, columns, numTuples);
    this.operatorID = oID;
    this.fromWorkerID = workerID;
    Objects.requireNonNull(columns);
  }

  // EOS
  public ExchangeTupleBatch(ExchangePairID oID, int workerID, Schema inputSchema) {
    this.dataHolder = null;
    this.operatorID = oID;
    this.fromWorkerID = workerID;
  }

  /**
   * Get the ParallelOperatorID, to which this message is targeted
   * */
  public ExchangePairID getOperatorID() {
    return this.operatorID;
  }

  /**
   * Get the worker id from which the message was sent
   * */
  public int getWorkerID() {
    return this.fromWorkerID;
  }

  public boolean isEos() {
    return this.dataHolder == null;
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public Schema outputSchema() {
    // return this.dataHolder.outputSchema();
    return this.dataHolder.outputSchema();
  }

  @Override
  public Schema inputSchema() {
    // return this.dataHolder.inputSchema();
    return this.dataHolder.inputSchema();
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
  public _TupleBatch renameColumn(int inputColumnIdx, String newName) {
    return this.dataHolder.renameColumn(inputColumnIdx, newName);
  }

  @Override
  public _TupleBatch filter(int fieldIdx, Op op, Object operand) {
    return this.dataHolder.filter(fieldIdx, op, operand);
  }

  @Override
  public _TupleBatch purgeFilters() {
    return this.dataHolder.purgeFilters();
  }

  @Override
  public _TupleBatch project(int[] remainingColumns) {
    return this.dataHolder.project(remainingColumns);
  }

  @Override
  public _TupleBatch purgeProjects() {
    return this.dataHolder.purgeProjects();
  }

  @Override
  public _TupleBatch append(_TupleBatch another) {
    return this.dataHolder.append(another);
  }

  @Override
  public _TupleBatch join(_TupleBatch other, Predicate p, _TupleBatch output) {
    return this.dataHolder.join(other,p,output);
  }

  @Override
  public _TupleBatch union(_TupleBatch another) {
    return this.dataHolder.union(another);
  }

  @Override
  public _TupleBatch intersect(_TupleBatch another) {
    return this.dataHolder.intersect(another);
  }

  @Override
  public _TupleBatch except(_TupleBatch another) {
    return this.dataHolder.except(another);
  }

  @Override
  public _TupleBatch distinct() {
    return this.dataHolder.distinct();
  }

  @Override
  public _TupleBatch groupby() {
    return this.dataHolder.groupby();
  }

  @Override
  public _TupleBatch orderby() {
    return this.dataHolder.orderby();
  }

  @Override
  public boolean getBoolean(int column, int row) {
    return this.dataHolder.getBoolean(column,row);
  }

  @Override
  public double getDouble(int column, int row) {
    return this.dataHolder.getDouble(column,row);
  }

  @Override
  public float getFloat(int column, int row) {
    return this.dataHolder.getFloat(column,row);
  }

  @Override
  public int getInt(int column, int row) {
    return this.dataHolder.getInt(column,row);
  }

  @Override
  public long getLong(int column, int row) {
    return this.dataHolder.getLong(column,row);
  }

  @Override
  public String getString(int column, int row) {
    return this.dataHolder.getString(column,row);
  }

  @Override
  public List<Column> outputRawData() {
    return this.dataHolder.outputRawData();
  }

  @Override
  public TupleBatchBuffer[] partition(PartitionFunction<?, ?> p, TupleBatchBuffer[] buffers) {
    return this.dataHolder.partition(p,buffers);
  }

  @Override
  public _TupleBatch remove(int innerIdx) {
    return this.dataHolder.remove(innerIdx);
  }

  @Override
  public int hashCode(int rowIndx) {
    return this.dataHolder.hashCode(rowIndx);
  }

}
