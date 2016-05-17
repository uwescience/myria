package edu.washington.escience.myria.mrbenchmarks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.StringColumn;
import edu.washington.escience.myria.column.builder.StringColumnBuilder;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;

public class SubStr extends UnaryOperator {

  public SubStr(final int substrColumnIdx, int fromCharIdx, int endCharIdx) {
    this(null, substrColumnIdx, fromCharIdx, endCharIdx);
  }

  public SubStr(final Operator child, final int substrColumnIdx, int fromCharIdx, int endCharIdx) {
    super(child);
    this.substrColumnIdx = substrColumnIdx;
    this.fromCharIdx = fromCharIdx;
    this.endCharIdx = endCharIdx;
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1471148052154135619L;

  @Override
  protected void init(ImmutableMap<String, Object> execEnvVars) throws DbException {}

  @Override
  protected void cleanup() throws DbException {}

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();
    TupleBatch tb = child.nextReady();
    if (tb != null) {
      StringColumnBuilder builder = new StringColumnBuilder();
      builder.expandAll();
      ImmutableList<? extends Column<?>> source = tb.getDataColumns();
      for (int idx = 0; idx < tb.numTuples(); ++idx) {
        String subStr =
            source.get(substrColumnIdx).getString(idx).substring(fromCharIdx, endCharIdx);
        builder.replaceString(subStr, idx);
      }

      StringColumn sc = builder.build();
      ImmutableList.Builder<Column<?>> newColumnsB = ImmutableList.builder();
      for (int i = 0; i < source.size(); i++) {
        if (i != substrColumnIdx) {
          newColumnsB.add(source.get(i));
        } else {
          newColumnsB.add(sc);
        }
      }
      tb = new TupleBatch(child.getSchema(), newColumnsB.build(), tb.numTuples());
    }
    return tb;
  }

  @Override
  public Schema generateSchema() {
    return null;
  }

  private final int substrColumnIdx;
  private final int fromCharIdx;
  private final int endCharIdx;
}
