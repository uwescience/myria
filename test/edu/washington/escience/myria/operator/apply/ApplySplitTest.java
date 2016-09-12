package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.SplitExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.BatchTupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class ApplySplitTest {

  private final String SEPARATOR = ",";
  private final long EXPECTED_RESULTS = 2 * TupleBatch.BATCH_SIZE + 1;

  @Test
  public void testApply() throws DbException {
    final Schema schema = Schema.ofFields("joined_ints", Type.STRING_TYPE);
    final Schema expectedResultSchema = Schema.ofFields("joined_ints_splits", Type.STRING_TYPE);
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < EXPECTED_RESULTS; ++i) {
      sb.append(i);
      if (i < EXPECTED_RESULTS - 1) {
        sb.append(SEPARATOR);
      }
    }
    input.putString(0, sb.toString());

    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    ExpressionOperator colIdx = new VariableExpression(0);
    ExpressionOperator regex = new ConstantExpression(SEPARATOR);
    ExpressionOperator split = new SplitExpression(colIdx, regex);
    Expression expr = new Expression("joined_ints_splits", split);
    Expressions.add(expr);

    Apply apply = new Apply(new BatchTupleSource(input), Expressions.build());
    apply.open(TestEnvVars.get());
    long rowIdx = 0;
    while (!apply.eos()) {
      TupleBatch result = apply.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());

        for (int batchIdx = 0; batchIdx < result.numTuples(); ++batchIdx, ++rowIdx) {
          assertEquals(rowIdx, Integer.parseInt(result.getString(0, batchIdx)));
        }
      }
    }
    assertEquals(EXPECTED_RESULTS, rowIdx);
    apply.close();
  }
}
