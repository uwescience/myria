package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.CounterExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.SplitExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.FlatteningApply;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class FlatteningApplyTest {

  private final String SEPARATOR = ",";
  private final long SPLIT_MAX = 10;
  private final long COUNTER_MAX = 2 * TupleBatch.BATCH_SIZE + 1;
  private final long EXPECTED_RESULTS = SPLIT_MAX * COUNTER_MAX;

  @Test
  public void testApply() throws DbException {
    final Schema schema = Schema.ofFields("long_count", Type.LONG_TYPE, "joined_ints", Type.STRING_TYPE);
    final Schema expectedResultSchema =
        Schema.ofFields("long_count", Type.LONG_TYPE, "joined_ints", Type.STRING_TYPE, "long_values", Type.LONG_TYPE,
            "joined_ints_splits", Type.STRING_TYPE);
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < SPLIT_MAX; ++i) {
      sb.append(i);
      if (i < SPLIT_MAX - 1) {
        sb.append(SEPARATOR);
      }
    }
    final String joinedInts = sb.toString();

    input.putLong(0, COUNTER_MAX);
    input.putString(1, joinedInts);
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();

    ExpressionOperator countColIdx = new VariableExpression(0);
    ExpressionOperator counter = new CounterExpression(countColIdx);
    Expressions.add(new Expression("long_values", counter));

    ExpressionOperator splitColIdx = new VariableExpression(1);
    ExpressionOperator regex = new ConstantExpression(SEPARATOR);
    ExpressionOperator split = new SplitExpression(splitColIdx, regex);
    Expressions.add(new Expression("joined_ints_splits", split));

    FlatteningApply apply = new FlatteningApply(new TupleSource(input), Expressions.build(), new int[] { 0, 1 });
    apply.open(TestEnvVars.get());
    int rowIdx = 0;
    while (!apply.eos()) {
      TupleBatch result = apply.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());

        for (int batchIdx = 0; batchIdx < result.numTuples(); ++batchIdx, ++rowIdx) {
          assertEquals(result.getLong(0, batchIdx), COUNTER_MAX);
          assertEquals(result.getString(1, batchIdx), joinedInts);
          assertEquals((rowIdx / SPLIT_MAX), result.getLong(2, batchIdx));
          assertEquals((rowIdx % SPLIT_MAX), Integer.parseInt(result.getString(3, batchIdx)));
        }
      }
    }
    assertEquals(EXPECTED_RESULTS, rowIdx);
    apply.close();
  }
}
