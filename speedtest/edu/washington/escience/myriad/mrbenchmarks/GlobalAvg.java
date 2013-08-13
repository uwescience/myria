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
import edu.washington.escience.myriad.operator.UnaryOperator;

public class GlobalAvg extends UnaryOperator {

  public GlobalAvg(int sumIdx, int countIdx) {
    this(null, sumIdx, countIdx);
  }

  public GlobalAvg(Operator child, int sumIdx, int countIdx) {
    super(child);
    this.sumIdx = sumIdx;
    this.countIdx = countIdx;
  }

  /**
   * Required for Java serialization.
   */
  private static final long serialVersionUID = 191438462118946730L;

  @Override
  protected void init(ImmutableMap<String, Object> execEnvVars) throws DbException {
    Schema cs = getChild().getSchema();
    ImmutableList.Builder<String> newNamesB = ImmutableList.builder();
    newNamesB.addAll(cs.getColumnNames());
    ImmutableList.Builder<Type> newTypesB = ImmutableList.builder();
    newTypesB.addAll(cs.getColumnTypes());
    schema = Schema.of(newTypesB.build(), newNamesB.build());
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();
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

      tb = new TupleBatch(schema, newColumnsB.build(), tb.getValidTuples());
    }
    return tb;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  private Schema schema;

  private final int sumIdx;
  private final int countIdx;
}
