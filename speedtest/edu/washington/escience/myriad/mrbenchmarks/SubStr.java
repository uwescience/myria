package edu.washington.escience.myriad.mrbenchmarks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.StringColumn;
import edu.washington.escience.myriad.column.StringColumnBuilder;
import edu.washington.escience.myriad.operator.Operator;

public class SubStr extends Operator {

  public SubStr(final int substrColumnIdx, int fromCharIdx, int endCharIdx) {
    this.substrColumnIdx = substrColumnIdx;
    this.fromCharIdx = fromCharIdx;
    this.endCharIdx = endCharIdx;
  }

  /**
     * 
     */
  private static final long serialVersionUID = 1471148052154135619L;

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
      StringColumnBuilder builder = new StringColumnBuilder();
      builder.expandAll();
      ImmutableList<Column<?>> source = tb.getDataColumns();
      for (Integer idx : tb.getValidIndices()) {
        String subStr = ((StringColumn) source.get(substrColumnIdx)).get(idx).substring(fromCharIdx, endCharIdx);
        builder.replace(idx, subStr);
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
      tb = new TupleBatch(child.getSchema(), newColumnsB.build(), tb.getValidTuples());
    }
    return tb;
  }

  @Override
  public Schema getSchema() {
    return null;
  }

  @Override
  public void setChildren(Operator[] children) {
    child = children[0];
  }

  private Operator child;
  private final int substrColumnIdx;
  private final int fromCharIdx;
  private final int endCharIdx;

}
