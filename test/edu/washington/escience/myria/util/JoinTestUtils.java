package edu.washington.escience.myria.util;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public final class JoinTestUtils {

  public static Schema leftSchema =
      Schema.ofFields(
          "left1", Type.LONG_TYPE, "left2", Type.BOOLEAN_TYPE, "left3", Type.STRING_TYPE);
  public static Schema rightSchema =
      Schema.ofFields(
          "right1", Type.STRING_TYPE, "right2", Type.LONG_TYPE, "right3", Type.BOOLEAN_TYPE);
  public static ImmutableList<TupleBatch> leftInput = ImmutableList.copyOf(getLeftInput());
  public static ImmutableList<TupleBatch> rightInput = ImmutableList.copyOf(getRightInput());

  /** Utility class can't be constructed. */
  private JoinTestUtils() {}

  private static List<TupleBatch> getLeftInput() {
    TupleBatchBuffer tbb = new TupleBatchBuffer(leftSchema);
    List<TupleBatch> ret = Lists.newLinkedList();

    tbb.putLong(0, 10L);
    tbb.putBoolean(1, true);
    tbb.putString(2, "value1");

    tbb.putLong(0, 15L);
    tbb.putBoolean(1, false);
    tbb.putString(2, "value2");

    tbb.putLong(0, 11L);
    tbb.putBoolean(1, false);
    tbb.putString(2, "value");

    ret.add(tbb.popAny());

    tbb.putLong(0, -11L);
    tbb.putBoolean(1, false);
    tbb.putString(2, "value3");

    tbb.putLong(0, -11L);
    tbb.putBoolean(1, false);
    tbb.putString(2, "value3");

    ret.add(tbb.popAny());

    return ret;
  }

  private static List<TupleBatch> getRightInput() {
    TupleBatchBuffer tbb = new TupleBatchBuffer(rightSchema);
    List<TupleBatch> ret = Lists.newLinkedList();

    tbb.putString(0, "value");
    tbb.putLong(1, 11L);
    tbb.putBoolean(2, false);

    ret.add(tbb.popAny());

    tbb.putString(0, "value2");
    tbb.putLong(1, 15L);
    tbb.putBoolean(2, false);

    ret.add(tbb.popAny());

    tbb.putString(0, "value3");
    tbb.putLong(1, -14L);
    tbb.putBoolean(2, false);

    tbb.putString(0, "value1");
    tbb.putLong(1, 10L);
    tbb.putBoolean(2, true);

    tbb.putString(0, "value3");
    tbb.putLong(1, -11L);
    tbb.putBoolean(2, false);

    tbb.putString(0, "value3");
    tbb.putLong(1, -11L);
    tbb.putBoolean(2, false);

    ret.add(tbb.popAny());

    return ret;
  }
}
