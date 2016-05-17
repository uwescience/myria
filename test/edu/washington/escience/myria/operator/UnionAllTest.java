package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.CastExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.TypeExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;
import edu.washington.escience.myria.util.TestUtils;

public class UnionAllTest {

  @Test
  public void testUnionAllConstructorWithNull() throws DbException {
    TupleSource[] children = new TupleSource[1];
    children[0] = new TupleSource(TestUtils.generateRandomTuples(10, 1000, false));
    UnionAll union = new UnionAll(null);
    union.setChildren(children);
  }

  @Test
  public void testUnionAllCorrectTuples() throws DbException {
    TupleBatchBuffer[] randomTuples = new TupleBatchBuffer[2];
    randomTuples[0] = TestUtils.generateRandomTuples(12300, 5000, false);
    randomTuples[1] = TestUtils.generateRandomTuples(4200, 2000, false);

    TupleSource[] children = new TupleSource[2];
    children[0] = new TupleSource(randomTuples[0]);
    children[1] = new TupleSource(randomTuples[1]);

    UnionAll union = new UnionAll(children);
    union.open(null);
    TupleBatch tb;

    Multiset<Long> actualCounts = HashMultiset.create();
    while (!union.eos()) {
      tb = union.nextReady();
      if (tb != null) {
        assertEquals(union.getSchema(), tb.getSchema());
        for (int i = 0; i < tb.numTuples(); i++) {
          long index = tb.getLong(0, i);
          actualCounts.add(index);
        }
      }
    }
    union.close();

    Multiset<Long> expectedCounts = HashMultiset.create();
    for (TupleBatchBuffer randomTuple : randomTuples) {
      for (TupleBatch tuples : randomTuple.getAll()) {
        for (int j = 0; j < tuples.numTuples(); j++) {
          Long index = tuples.getLong(0, j);
          expectedCounts.add(index);
        }
      }
    }

    for (Multiset.Entry<Long> expectedEntry : expectedCounts.entrySet()) {
      assertEquals(expectedEntry.getCount(), actualCounts.count(expectedEntry.getElement()));
    }
  }

  @Test
  public void testUnionAllCount() throws DbException {
    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(TestUtils.generateRandomTuples(12300, 5000, false));
    children[1] = new TupleSource(TestUtils.generateRandomTuples(4200, 2000, false));
    children[2] = new TupleSource(TestUtils.generateRandomTuples(19900, 5000, false));
    UnionAll union = new UnionAll(children);
    union.open(null);
    TupleBatch tb = null;
    int count = 0;
    while (!union.eos()) {
      tb = union.nextReady();
      if (tb != null) {
        assertEquals(union.getSchema(), tb.getSchema());
        count += tb.numTuples();
      }
    }
    union.close();
    assertEquals(12300 + 4200 + 19900, count);
  }

  @Test
  public void testUnionAllVaryingSchemas() throws DbException {
    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(TestUtils.generateRandomTuples(12300, 5000, false));
    children[1] = new TupleSource(TestUtils.generateRandomTuples(4200, 2000, false));

    /* Child 2 will have tuples with different names */
    TupleBatchBuffer tuples2 = TestUtils.generateRandomTuples(19900, 5000, false);
    Schema normalSchema = tuples2.getSchema();
    List<String> renames = new LinkedList<>();
    for (String s : tuples2.getSchema().getColumnNames()) {
      renames.add(s + "_2only");
    }
    Schema renamedSchema = new Schema(normalSchema.getColumnTypes(), renames);
    TupleBatchBuffer tuples2renamed = new TupleBatchBuffer(renamedSchema);
    for (TupleBatch tb : tuples2.getAll()) {
      tuples2renamed.appendTB(tb.rename(renames));
    }
    children[2] = new TupleSource(tuples2renamed);

    UnionAll union = new UnionAll(children);
    union.open(null);
    TupleBatch tb = null;
    int count = 0;
    while (!union.eos()) {
      tb = union.nextReady();
      if (tb != null) {
        assertEquals(union.getSchema(), tb.getSchema());
        count += tb.numTuples();
      }
    }
    union.close();
    assertEquals(12300 + 4200 + 19900, count);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnionIncompatibleSchemas() throws DbException {
    Operator[] children = new Operator[3];
    // range always returns INT_TYPE
    children[0] = new TupleSource(TestUtils.range(5));
    children[1] = new TupleSource(TestUtils.range(50));

    /* Child 2 will have tuples with different type -- cast int to long */
    children[2] =
        new Apply(
            new TupleSource(TestUtils.range(50)),
            ImmutableList.of(
                new Expression(
                    "long",
                    new CastExpression(
                        new VariableExpression(0), new TypeExpression(Type.LONG_TYPE)))));

    UnionAll union = new UnionAll(children);
    union.open(TestEnvVars.get(2));
  }
}
