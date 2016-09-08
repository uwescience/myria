package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.SequenceExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class ApplySequenceTest {
  private final long COUNT = 2 * TupleBatch.BATCH_SIZE + 1;

  @Test
  public void testApply() throws DbException {
    final Schema schema = Schema.ofFields("int_count", Type.LONG_TYPE);
    final Schema expectedResultSchema = Schema.ofFields("int_values", Type.LONG_TYPE);
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);

    input.putLong(0, COUNT);

    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    ExpressionOperator colIdx = new VariableExpression(0);
    ExpressionOperator split = new SequenceExpression(colIdx);
    Expression expr = new Expression("int_values", split);
    Expressions.add(expr);

    Apply apply = new Apply(new TupleSource(input), Expressions.build());
    apply.open(TestEnvVars.get());
    int rowIdx = 0;
    while (!apply.eos()) {
      TupleBatch result = apply.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());

        for (int batchIdx = 0; batchIdx < result.numTuples(); ++batchIdx, ++rowIdx) {
          assertEquals(rowIdx, result.getLong(0, batchIdx));
        }
      }
    }
    assertEquals(COUNT, rowIdx);
    apply.close();
  }
}
