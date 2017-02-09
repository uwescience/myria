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
import edu.washington.escience.myria.expression.NgramExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.BatchTupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.TestEnvVars;

public class ApplyNgramTest {

  private static final long EXPECTED_RESULTS = 2 * TupleUtils.getBatchSize(Type.STRING_TYPE) + 1;
  private static final long NGRAM_LEN = 3;
  private static final long CHAR_SEQ_LEN = EXPECTED_RESULTS + NGRAM_LEN - 1;

  @Test
  public void testApply() throws DbException {
    final Schema schema = Schema.ofFields("char_sequence", Type.STRING_TYPE);
    final Schema expectedResultSchema = Schema.ofFields("ngrams", Type.STRING_TYPE);
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < CHAR_SEQ_LEN; ++i) {
      sb.append((char) i);
    }
    input.putString(0, sb.toString());

    ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
    ExpressionOperator colIdx = new VariableExpression(0);
    ExpressionOperator ngramLen = new ConstantExpression(NGRAM_LEN);
    ExpressionOperator split = new NgramExpression(colIdx, ngramLen);
    Expression expr = new Expression("ngrams", split);
    expressions.add(expr);

    Apply apply = new Apply(new BatchTupleSource(input), expressions.build());
    apply.open(TestEnvVars.get());
    long rowIdx = 0;
    while (!apply.eos()) {
      TupleBatch result = apply.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());

        for (int batchIdx = 0; batchIdx < result.numTuples(); ++batchIdx, ++rowIdx) {
          char[] ngramChars = new char[] {(char) rowIdx, (char) (rowIdx + 1), (char) (rowIdx + 2)};
          String ngram = new String(ngramChars);
          assertEquals(ngram, result.getString(0, batchIdx));
        }
      }
    }
    assertEquals(EXPECTED_RESULTS, rowIdx);
    apply.close();
  }

  @Test
  public void testApplywithCounter() throws DbException {
    final Schema schema = Schema.ofFields("char_sequence", Type.STRING_TYPE);
    final Schema expectedResultSchema =
        Schema.ofFields("ngrams", Type.STRING_TYPE, "flatmapid", Type.INT_TYPE);
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < CHAR_SEQ_LEN; ++i) {
      sb.append((char) i);
    }
    input.putString(0, sb.toString());

    ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
    ExpressionOperator colIdx = new VariableExpression(0);
    ExpressionOperator ngramLen = new ConstantExpression(NGRAM_LEN);
    ExpressionOperator split = new NgramExpression(colIdx, ngramLen);
    Expression expr = new Expression("ngrams", split);
    expressions.add(expr);

    Apply apply = new Apply(new BatchTupleSource(input), expressions.build(), true);
    apply.open(TestEnvVars.get());
    long rowIdx = 0;
    while (!apply.eos()) {
      TupleBatch result = apply.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());

        for (int batchIdx = 0; batchIdx < result.numTuples(); ++batchIdx, ++rowIdx) {
          char[] ngramChars = new char[] {(char) rowIdx, (char) (rowIdx + 1), (char) (rowIdx + 2)};
          String ngram = new String(ngramChars);
          int fltmapid = (int) rowIdx;
          assertEquals(ngram, result.getString(0, batchIdx));
          assertEquals(fltmapid, result.getInt(1, batchIdx));
        }
      }
    }
    assertEquals(EXPECTED_RESULTS, rowIdx);
    apply.close();
  }
}
