package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.ExpressionEncoding;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.SqrtExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.TupleSource;

public class ApplyTest {

  private final int NUM_TUPLES = 20000;

  @Test
  public void testApplySqrt() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.LONG_TYPE, Type.DOUBLE_TYPE), ImmutableList.of("a", "b"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < NUM_TUPLES; i++) {
      tbb.put(0, (long) Math.pow(i, 2));
      tbb.put(1, (double) i);
    }
    ImmutableList.Builder<Expression> expressions = ImmutableList.builder();

    // Expression: Math.sqrt(col0);

    ExpressionOperator var = new VariableExpression(0);
    ExpressionOperator root = new SqrtExpression(var);

    ExpressionEncoding exprEnc = new ExpressionEncoding("squareroot", root);

    // System.out.println(exprEnc.construct());

    expressions.add(exprEnc.construct());
    Apply apply = new Apply(new TupleSource(tbb), expressions.build());
    apply.open(null);
    TupleBatch result;
    int resultSize = 0;
    while (!apply.eos()) {
      result = apply.nextReady();
      if (result != null) {
        assertEquals(1, result.getSchema().numColumns());
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(0));
        for (int i = 0; i < result.numTuples(); i++) {
          assertEquals(i + resultSize, result.getDouble(0, i), 0.0000001);
        }
        resultSize += result.numTuples();
      }
    }
    assertEquals(NUM_TUPLES, resultSize);
    apply.close();
  }
}
