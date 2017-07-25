package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.BitsetExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.BatchTupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class ApplyBitsetTest {

  @Test
  public void testApply() throws DbException {
    final Schema schema = Schema.ofFields("bytes", Type.BLOB_TYPE);
    final Schema expectedResultSchema = Schema.ofFields("bits", Type.BOOLEAN_TYPE);
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);

    byte[] bytes = new byte[8];
    for (int i = 0; i < 8; ++i) {
      bytes[i] = (byte) (1 << i);
    }
    input.putBlob(0, ByteBuffer.wrap(bytes));

    ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
    ExpressionOperator colIdx = new VariableExpression(0);
    ExpressionOperator bits = new BitsetExpression(colIdx);
    Expression expr = new Expression("bits", bits);
    expressions.add(expr);

    Apply apply = new Apply(new BatchTupleSource(input), expressions.build());
    apply.open(TestEnvVars.get());
    int rowIdx = 0;
    while (!apply.eos()) {
      TupleBatch result = apply.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());

        for (int batchIdx = 0; batchIdx < result.numTuples(); ++batchIdx, ++rowIdx) {
          int srcByteIdx = rowIdx / 8;
          int srcBitIdx = rowIdx % 8;
          boolean trueExpected = (srcByteIdx == srcBitIdx);
          assertEquals(trueExpected, result.getBoolean(0, batchIdx));
        }
      }
    }
    assertEquals(8 * 8, rowIdx);
    apply.close();
  }
}
