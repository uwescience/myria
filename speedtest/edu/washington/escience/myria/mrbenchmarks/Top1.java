package edu.washington.escience.myria.mrbenchmarks;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public class Top1 extends UnaryOperator {

  public Top1(int toCompareColumnIdx) {
    this(null, toCompareColumnIdx);
  }

  public Top1(final Operator child, final int toCompareColumnIdx) {
    super(child);
    this.toCompareColumnIdx = toCompareColumnIdx;
  }

  /**
   *
   */
  private static final long serialVersionUID = 191438462118946730L;

  @Override
  protected void init(ImmutableMap<String, Object> execEnvVars) throws DbException {
    compareType = getSchema().getColumnType(toCompareColumnIdx);
  }

  @Override
  protected void cleanup() throws DbException {}

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();
    TupleBatch tb = null;
    while ((tb = child.nextReady()) != null) {
      for (int i = 0; i < tb.numTuples(); i++) {
        switch (compareType) {
          case INT_TYPE:
            int c = tb.getInt(toCompareColumnIdx, i);
            if (currentTopInt > c) {
              currentTopInt = c;
              currentTopTB = tb;
              currentTopIdx = i;
            }
            break;
          case LONG_TYPE:
            long cl = tb.getLong(toCompareColumnIdx, i);
            if (currentTopLong > cl) {
              currentTopLong = cl;
              currentTopTB = tb;
              currentTopIdx = i;
            }
            break;
          case FLOAT_TYPE:
            float cf = tb.getFloat(toCompareColumnIdx, i);
            if (currentTopFloat > cf) {
              currentTopFloat = cf;
              currentTopTB = tb;
              currentTopIdx = i;
            }
            break;
          case DOUBLE_TYPE:
            double cd = tb.getDouble(toCompareColumnIdx, i);
            if (currentTopDouble > cd) {
              currentTopDouble = cd;
              currentTopTB = tb;
              currentTopIdx = i;
            }
            break;
          case STRING_TYPE:
            String cs = tb.getString(toCompareColumnIdx, i);
            if (currentTopString == null) {
              currentTopString = cs;
              currentTopTB = tb;
              currentTopIdx = i;
            } else if (currentTopString.compareTo(cs) > 0) {
              currentTopString = cs;
              currentTopTB = tb;
              currentTopIdx = i;
            }
            break;
          case DATETIME_TYPE:
            DateTime tt = tb.getDateTime(toCompareColumnIdx, i);
            if (currentTopDate == null) {
              currentTopDate = tt;
              currentTopTB = tb;
              currentTopIdx = i;
            } else if (currentTopDate.compareTo(tt) > 0) {
              currentTopDate = tt;
              currentTopTB = tb;
              currentTopIdx = i;
            }
            break;
          default:
            throw new UnsupportedOperationException("Not supported for type: " + compareType);
        }
      }
    }

    if (child.eos() || child.eoi()) {
      if (currentTopTB != null) {
        TupleBatchBuffer tbb = new TupleBatchBuffer(getSchema());
        for (int i = 0; i < getSchema().numColumns(); i++) {
          tbb.put(i, currentTopTB.getDataColumns().get(i), currentTopIdx);
        }
        currentTopTB = null;
        return tbb.popAny();
      }
    }
    return null;
  }

  @Override
  protected Schema generateSchema() {
    final Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }

  private Type compareType;
  private TupleBatch currentTopTB;
  private int currentTopIdx;

  private final int toCompareColumnIdx;
  private int currentTopInt = Integer.MAX_VALUE;
  private long currentTopLong = Long.MAX_VALUE;
  private double currentTopDouble = Double.MAX_VALUE;
  private float currentTopFloat = Float.MAX_VALUE;
  private String currentTopString = null;
  private DateTime currentTopDate = null;
}
