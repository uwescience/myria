package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.SequenceExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.SplitExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.BatchTupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.TestEnvVars;

public class FlatteningApplyTest {

  private final String SEPARATOR = ",";
  private final int SPLIT_MAX = 10;
  private final long COUNTER_MAX = 2 * TupleUtils.get_Batch_size(Type.LONG_TYPE) + 1;
  private final long EXPECTED_RESULTS = SPLIT_MAX * COUNTER_MAX;

  @Test
  public void testApply() throws DbException {
    final Schema schema =
        Schema.ofFields(
            "int_count",
            Type.LONG_TYPE,
            "ignore_1",
            Type.FLOAT_TYPE,
            "joined_ints",
            Type.STRING_TYPE,
            "ignore_2",
            Type.BOOLEAN_TYPE);
    final Schema expectedResultSchema =
        Schema.ofFields(
            "int_count",
            Type.LONG_TYPE,
            "joined_ints",
            Type.STRING_TYPE,
            "int_values",
            Type.LONG_TYPE,
            "joined_ints_splits",
            Type.STRING_TYPE);
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
    input.putFloat(1, 1.0f);
    input.putString(2, joinedInts);
    input.putBoolean(3, true);
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();

    ExpressionOperator countColIdx = new VariableExpression(0);
    Expressions.add(new Expression("int_count", countColIdx));

    ExpressionOperator splitColIdx = new VariableExpression(2);
    Expressions.add(new Expression("joined_ints", splitColIdx));

    ExpressionOperator counter = new SequenceExpression(countColIdx);
    Expressions.add(new Expression("int_values", counter));

    ExpressionOperator regex = new ConstantExpression(SEPARATOR);
    ExpressionOperator split = new SplitExpression(splitColIdx, regex);
    Expressions.add(new Expression("joined_ints_splits", split));

    Apply apply = new Apply(new BatchTupleSource(input), Expressions.build());
    apply.open(TestEnvVars.get());
    int rowIdx = 0;
    while (!apply.eos()) {
      TupleBatch result = apply.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());

        for (int batchIdx = 0; batchIdx < result.numTuples(); ++batchIdx, ++rowIdx) {
          assertEquals(COUNTER_MAX, result.getLong(0, batchIdx));
          assertEquals(joinedInts, result.getString(1, batchIdx));
          assertEquals((rowIdx / SPLIT_MAX), result.getLong(2, batchIdx));
          assertEquals((rowIdx % SPLIT_MAX), Integer.parseInt(result.getString(3, batchIdx)));
        }
      }
    }
    assertEquals(EXPECTED_RESULTS, rowIdx);
    apply.close();
  }
}
