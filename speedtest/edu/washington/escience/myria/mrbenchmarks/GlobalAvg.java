package edu.washington.escience.myria.mrbenchmarks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.DoubleColumnBuilder;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;

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
  protected void init(ImmutableMap<String, Object> execEnvVars) throws DbException {}

  @Override
  protected void cleanup() throws DbException {}

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();
    TupleBatch tb = child.nextReady();
    if (tb != null) {
      ImmutableList<? extends Column<?>> inputColumns = tb.getDataColumns();
      DoubleColumnBuilder rc = new DoubleColumnBuilder();
      rc.expandAll();
      for (int idx = 0; idx < tb.numTuples(); ++idx) {
        rc.replaceDouble(
            inputColumns.get(sumIdx).getLong(idx) * 1.0 / inputColumns.get(countIdx).getLong(idx),
            idx);
      }

      ImmutableList.Builder<Column<?>> newColumnsB = ImmutableList.builder();
      newColumnsB.addAll(inputColumns);
      newColumnsB.add(rc.build());

      tb = new TupleBatch(getSchema(), newColumnsB.build(), tb.numTuples());
    }
    return tb;
  }

  @Override
  public Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    Schema cs = getChild().getSchema();
    if (cs == null) {
      return null;
    }
    ImmutableList.Builder<String> newNamesB = ImmutableList.builder();
    newNamesB.addAll(cs.getColumnNames());
    ImmutableList.Builder<Type> newTypesB = ImmutableList.builder();
    newTypesB.addAll(cs.getColumnTypes());
    return Schema.of(newTypesB.build(), newNamesB.build());
  }

  private final int sumIdx;
  private final int countIdx;
}
