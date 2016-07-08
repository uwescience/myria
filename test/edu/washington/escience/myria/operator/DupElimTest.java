package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.List;

import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class DupElimTest {
  private static List<TupleBatch> makeTestData() {
    List<Type> types =
        ImmutableList.of(
            Type.BOOLEAN_TYPE, Type.DATETIME_TYPE, Type.INT_TYPE, Type.LONG_TYPE, Type.STRING_TYPE);
    List<String> names = ImmutableList.of("boolean", "datetime", "int", "long", "string");
    Schema schema = Schema.of(types, names);
    TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    List<TupleBatch> ret = Lists.newLinkedList();
    boolean bool0 = false;
    DateTime dateTime0 = DateTime.now();
    int int0 = 17;
    long long0 = 4000000000L;
    long long1 = -4000000000L;
    String string0 = "row0";
    String string1 = "row1";

    assertNotEquals(long0, long1);
    assertNotEquals(string0, string1);

    /* First row */
    tbb.putBoolean(0, bool0);
    tbb.putDateTime(1, dateTime0);
    tbb.putInt(2, int0);
    tbb.putLong(3, long0);
    tbb.putString(4, string0);
    /* Second row is the same as the first row with one difference in column 3. */
    tbb.putBoolean(0, bool0);
    tbb.putDateTime(1, dateTime0);
    tbb.putInt(2, int0);
    tbb.putLong(3, long1);
    tbb.putString(4, string0);
    /* Third row is the same as the first. */
    tbb.putBoolean(0, bool0);
    tbb.putDateTime(1, dateTime0);
    tbb.putInt(2, int0);
    tbb.putLong(3, long0);
    tbb.putString(4, string0);
    /* Fourth row is the same as the second. */
    tbb.putBoolean(0, bool0);
    tbb.putDateTime(1, dateTime0);
    tbb.putInt(2, int0);
    tbb.putLong(3, long1);
    tbb.putString(4, string0);
    /* Fifth row is the same as the second. */
    tbb.putBoolean(0, bool0);
    tbb.putDateTime(1, dateTime0);
    tbb.putInt(2, int0);
    tbb.putLong(3, long1);
    tbb.putString(4, string0);
    /* Sixth row is different in column 4. */
    tbb.putBoolean(0, bool0);
    tbb.putDateTime(1, dateTime0);
    tbb.putInt(2, int0);
    tbb.putLong(3, long0);
    tbb.putString(4, string1);
    /* Pop a TB of 6 rows, 3 unique (0, 1, 5). */
    ret.add(tbb.popAny());
    /* Copy that tuplebatch twice more */
    ret.add(ret.get(0));
    ret.add(ret.get(0));
    /* Add a single tuple that's different from all prev in column 4 */
    tbb.putBoolean(0, bool0);
    tbb.putDateTime(1, dateTime0);
    tbb.putInt(2, int0);
    tbb.putLong(3, long1);
    tbb.putString(4, string1);
    /* Add it to ret. */
    ret.add(tbb.popAny());
    /* At the end we should have 4 different rows. */
    return ret;
  }

  @Test
  public void testDupElim() throws DbException {
    TupleSource src = new TupleSource(makeTestData());
    StreamingStateWrapper dupElim = new StreamingStateWrapper(src, new DupElim());

    List<TupleBatch> ans = Lists.newLinkedList();
    dupElim.open(TestEnvVars.get());
    while (!dupElim.eos()) {
      TupleBatch tb = dupElim.nextReady();
      if (tb != null) {
        ans.add(tb);
      }
    }
    dupElim.close();

    int count = 0;
    for (TupleBatch tb : ans) {
      count += tb.numTuples();
    }
    assertEquals(2, ans.size());
    assertEquals(4, count);
    assertEquals(3, ans.get(0).numTuples());
    assertEquals(1, ans.get(1).numTuples());
  }
}
