package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.IntDivideExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.StateExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.SingletonRelation;
import edu.washington.escience.myria.operator.StatefulApply;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class StatefulApplyTest {

  private final int NUM_TUPLES = 2 * TupleBatch.BATCH_SIZE;
  private final int SMALL_NUM_TUPLES = 10;

  @Test
  public void testStatefulApplyRange() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.STRING_TYPE), ImmutableList.of("name"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < NUM_TUPLES; i++) {
      tbb.putString(0, "Foo" + i);
    }

    Expression initializer =
        new Expression("counter", new ConstantExpression(Type.LONG_TYPE, "-1"));
    Expression expression = new Expression("index", new StateExpression(0));
    Expression increment =
        new Expression(
            new PlusExpression(
                new StateExpression(0), new ConstantExpression(Type.LONG_TYPE, "1")));

    ImmutableList.Builder<Expression> Initializers = ImmutableList.builder();
    Initializers.add(initializer);

    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    Expressions.add(expression);

    ImmutableList.Builder<Expression> Updaters = ImmutableList.builder();
    Updaters.add(increment);

    StatefulApply apply =
        new StatefulApply(
            new TupleSource(tbb), Expressions.build(), Initializers.build(), Updaters.build());

    apply.open(TestEnvVars.get());
    TupleBatch result;
    int resultSize = 0;
    while (!apply.eos()) {
      result = apply.nextReady();
      if (result != null) {
        assertEquals(1, result.getSchema().numColumns());
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(0));

        for (int curI = 0; curI < result.numTuples(); curI++) {
          long i = curI + resultSize;
          assertEquals(i, result.getLong(0, curI));
        }
        resultSize += result.numTuples();
      }
    }
    assertEquals(NUM_TUPLES, resultSize);
    apply.close();
  }

  @Test
  public void testStatefulApplyRunningMean() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("salary"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < SMALL_NUM_TUPLES; i++) {
      tbb.putLong(0, i + 1);
    }

    Expression initializeCounter =
        new Expression("counter", new ConstantExpression(Type.LONG_TYPE, "0"));
    Expression initializeSum = new Expression("sum", new ConstantExpression(Type.LONG_TYPE, "0"));
    Expression updateCounter =
        new Expression(
            new PlusExpression(
                new StateExpression(0), new ConstantExpression(Type.LONG_TYPE, "1")));
    Expression updateSum =
        new Expression(new PlusExpression(new StateExpression(1), new VariableExpression(0)));

    Expression avg =
        new Expression(
            "average", new IntDivideExpression(new StateExpression(1), new StateExpression(0)));

    ImmutableList.Builder<Expression> Initializers = ImmutableList.builder();
    Initializers.add(initializeCounter);
    Initializers.add(initializeSum);

    ImmutableList.Builder<Expression> Updaters = ImmutableList.builder();
    Updaters.add(updateCounter);
    Updaters.add(updateSum);

    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    Expressions.add(avg);
    Expressions.add(new Expression("number", new VariableExpression(0)));

    StatefulApply apply =
        new StatefulApply(
            new TupleSource(tbb), Expressions.build(), Initializers.build(), Updaters.build());

    apply.open(TestEnvVars.get());
    TupleBatch result;
    int resultSize = 0;
    while (!apply.eos()) {
      result = apply.nextReady();
      if (result != null) {
        assertEquals(2, result.getSchema().numColumns());
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(0));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(1));

        for (int curI = 0; curI < result.numTuples(); curI++) {
          long i = curI + resultSize + 1;
          long sum = i * (i + 1) / 2;
          long runningAverage = sum / i;
          assertEquals(runningAverage, result.getLong(0, curI));
        }
        resultSize += result.numTuples();
      }
    }
    assertEquals(SMALL_NUM_TUPLES, resultSize);
    apply.close();
  }

  @Test
  public void testCounterOnSingleton() throws DbException {
    SingletonRelation singleton = new SingletonRelation();
    Expression emitExpression = new Expression("x", new StateExpression(0));
    Expression initExpression = new Expression("cntr", new ConstantExpression(0));
    Expression updateExpression =
        new Expression(
            "cntr", new PlusExpression(new StateExpression(0), new ConstantExpression(1)));
    StatefulApply apply =
        new StatefulApply(
            singleton,
            ImmutableList.of(emitExpression),
            ImmutableList.of(initExpression),
            ImmutableList.of(updateExpression));

    apply.open(TestEnvVars.get());
    assertEquals("x", apply.getSchema().getColumnName(0));
    int count = 0;
    while (!apply.eos()) {
      TupleBatch tb = apply.nextReady();
      if (tb == null) {
        continue;
      }
      for (int i = 0; i < tb.numTuples(); ++i) {
        assertEquals(count + 1, tb.getInt(0, i));
        count++;
      }
    }
    apply.close();
    assertEquals(1, count);
  }

  @Test
  public void testBug547UpdateNotNeedsCompile() throws DbException {
    final Schema schema = Schema.ofFields(Type.INT_TYPE);
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < 3; ++i) {
      tbb.putInt(0, i);
    }

    Expression emitExpression = new Expression("x", new StateExpression(0));
    Expression initExpression = new Expression("old", new ConstantExpression(-1));
    Expression updateExpression = new Expression("old", new VariableExpression(0));
    StatefulApply apply =
        new StatefulApply(
            new TupleSource(tbb),
            ImmutableList.of(emitExpression),
            ImmutableList.of(initExpression),
            ImmutableList.of(updateExpression));

    apply.open(TestEnvVars.get());
    assertEquals("x", apply.getSchema().getColumnName(0));
    int old = -1;
    while (!apply.eos()) {
      TupleBatch tb = apply.nextReady();
      if (tb == null) {
        continue;
      }
      for (int i = 0; i < tb.numTuples(); ++i) {
        old++;
        assertEquals(old, tb.getInt(0, i));
      }
    }
    apply.close();
    assertEquals(2, old);
  }
}
