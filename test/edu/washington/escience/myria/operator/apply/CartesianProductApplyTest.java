package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Test;

import com.google.common.base.Joiner;
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

public class CartesianProductApplyTest {

  private final String SEPARATOR = ",";

  @Test
  public void testApply() throws DbException {
    final Schema schema = Schema.ofFields("col_1", Type.STRING_TYPE, "col_2", Type.STRING_TYPE);
    final Schema expectedResultSchema =
        Schema.ofFields("col_1_split", Type.STRING_TYPE, "col_2_split", Type.STRING_TYPE);
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);

    final ImmutableList<String> input1 = ImmutableList.of("a", "b", "c");
    final ImmutableList<String> input2 = ImmutableList.of("d", "e", "f");

    Stream<List<String>> inputPairsStream =
        input1.stream().flatMap(i1 -> input2.stream().map(i2 -> ImmutableList.of(i1, i2)));
    Iterator<List<String>> inputPairs = inputPairsStream.iterator();

    input.putString(0, Joiner.on(SEPARATOR).join(input1));
    input.putString(1, Joiner.on(SEPARATOR).join(input2));
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();

    ExpressionOperator regex = new ConstantExpression(SEPARATOR);
    ExpressionOperator col1splits = new SplitExpression(new VariableExpression(0), regex);
    ExpressionOperator col2splits = new SplitExpression(new VariableExpression(1), regex);
    Expressions.add(new Expression("col_1_split", col1splits));
    Expressions.add(new Expression("col_2_split", col2splits));

    Apply apply = new Apply(new BatchTupleSource(input), Expressions.build());
    apply.open(TestEnvVars.get());
    int rowIdx = 0;
    while (!apply.eos()) {
      TupleBatch result = apply.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());
        for (int batchIdx = 0; batchIdx < result.numTuples(); ++batchIdx, ++rowIdx) {
          List<String> pair = inputPairs.next();
          assertEquals(pair.get(0), result.getString(0, batchIdx));
          assertEquals(pair.get(1), result.getString(1, batchIdx));
        }
      }
    }
    assertEquals(input1.size() * input2.size(), rowIdx);
    apply.close();
  }
}
