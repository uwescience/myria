package edu.washington.escience.myria.operator;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class DifferenceTest {

  private TupleBatchBuffer leftTbb, rightTbb;

  @Before
  public void setUp() throws Exception {
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));
    leftTbb = new TupleBatchBuffer(schema);

    {
      long[] ids = new long[] {0, 2, 2, 3, 4, 5, 6, 8, 8, 8, 8, 10, 10, 10};
      String[] names =
          new String[] {"c", "c", "c", "b", "b", "b", "b", "a", "a", "a", "a", "a", "a", "a"};

      for (int i = 0; i < ids.length; i++) {
        leftTbb.putLong(0, ids[i]);
        leftTbb.putString(1, names[i]);
      }
    }

    rightTbb = new TupleBatchBuffer(schema);
    {
      long[] ids = new long[] {2, 10, 2, 9};
      String[] names = new String[] {"c", "a", "c", "k"};

      for (int i = 0; i < ids.length; i++) {
        rightTbb.putLong(0, ids[i]);
        rightTbb.putString(1, names[i]);
      }
    }
  }

  @Test
  public void test() throws Exception {
    TupleSource left = new TupleSource(leftTbb);
    TupleSource right = new TupleSource(rightTbb);

    BinaryOperator diff = new Difference(left, right);
    diff.open(null);

    TupleBatchBuffer result = new TupleBatchBuffer(diff.getSchema());
    TupleBatchBuffer expected = new TupleBatchBuffer(diff.getSchema());

    {
      long[] ids = new long[] {0, 3, 4, 5, 6, 8};
      String[] names = new String[] {"c", "b", "b", "b", "b", "a"};

      for (int i = 0; i < ids.length; i++) {
        expected.putLong(0, ids[i]);
        expected.putString(1, names[i]);
      }
    }

    while (!diff.eos()) {
      TupleBatch batch = diff.nextReady();
      if (batch != null) {
        batch.compactInto(result);
      }
    }

    diff.close();

    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(result);
    final HashMap<Tuple, Integer> expectedBag = TestUtils.tupleBatchToTupleBag(expected);
    TestUtils.assertTupleBagEqual(expectedBag, resultBag);
  }

  @Test(expected = DbException.class)
  public void incompatibleSchemas() throws DbException {
    final Schema s1 =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));
    Operator left = EmptyRelation.of(s1);

    final Schema s2 =
        new Schema(
            ImmutableList.of(Type.INT_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));
    Operator right = EmptyRelation.of(s2);

    BinaryOperator diff = new Difference(left, right);
    diff.open(null);
  }
}
