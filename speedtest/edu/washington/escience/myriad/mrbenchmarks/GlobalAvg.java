package edu.washington.escience.myriad.mrbenchmarks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.DoubleColumnBuilder;
import edu.washington.escience.myriad.column.LongColumn;
import edu.washington.escience.myriad.operator.Operator;

public class GlobalAvg extends Operator {

  public GlobalAvg(int sumIdx, int countIdx) {
    this.sumIdx = sumIdx;
    this.countIdx = countIdx;
  }

  /**
     * 
     */
  private static final long serialVersionUID = 191438462118946730L;

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  protected void init(ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {

    TupleBatch tb = child.nextReady();
    if (tb != null) {
      ImmutableList<Column<?>> inputColumns = tb.getDataColumns();
      DoubleColumnBuilder rc = new DoubleColumnBuilder();
      rc.expandAll();
      for (Integer idx : tb.getValidIndices()) {
        rc.replace(idx, ((LongColumn) inputColumns.get(sumIdx)).get(idx) * 1.0
            / ((LongColumn) inputColumns.get(countIdx)).get(idx));
      }

      ImmutableList.Builder<Column<?>> newColumnsB = ImmutableList.builder();
      newColumnsB.addAll(inputColumns);
      newColumnsB.add(rc.build());

      tb = new TupleBatch(s, newColumnsB.build(), tb.getValidTuples());
    }
    return tb;
  }

  @Override
  public Schema getSchema() {
    return s;
  }

  @Override
  public void setChildren(Operator[] children) {
    child = children[0];
    Schema cs = child.getSchema();
    ImmutableList.Builder<String> newNamesB = ImmutableList.builder();
    newNamesB.addAll(cs.getColumnNames());
    ImmutableList.Builder<Type> newTypesB = ImmutableList.builder();
    newTypesB.addAll(cs.getColumnTypes());
    s = Schema.of(newTypesB.build(), newNamesB.build());
  }

  private Operator child;
  private Schema s;

  private final int sumIdx;
  private final int countIdx;
}
