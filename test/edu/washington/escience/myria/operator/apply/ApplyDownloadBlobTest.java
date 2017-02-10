package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

import edu.washington.escience.myria.expression.DownloadBlobExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.SqrtExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.BatchTupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

import edu.washington.escience.myria.util.TestEnvVars;

public class ApplyDownloadBlobTest {

  @Test
  public void ApplyTest() throws DbException {
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.STRING_TYPE, Type.LONG_TYPE), ImmutableList.of("blobs", "b"));
    final Schema expectedResultSchema =
        new Schema(
            ImmutableList.of(Type.BLOB_TYPE, Type.DOUBLE_TYPE), ImmutableList.of("blobs", "sqrt"));

    final TupleBatchBuffer input = new TupleBatchBuffer(schema);
    input.putString(0, "https://s3-us-west-2.amazonaws.com/myria-test/blobdata.csv".toString());
    input.putLong(1, 2);
    input.putString(0, "https://s3-us-west-2.amazonaws.com/myria-test/blobdata.csv".toString());
    input.putLong(1, 2);

    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    ExpressionOperator filename = new VariableExpression(0);
    ;

    ExpressionOperator db = new DownloadBlobExpression(filename);
    Expression expr = new Expression("blobs", db);

    Expressions.add(expr);
    ExpressionOperator varb = new VariableExpression(1);
    ExpressionOperator squareRoot = new SqrtExpression(varb);

    Expression expr2 = new Expression("sqrt", squareRoot);
    Expressions.add(expr2);

    Apply apply = new Apply(new BatchTupleSource(input), Expressions.build(), false);
    apply.open(TestEnvVars.get());
    while (!apply.eos()) {
      TupleBatch result = apply.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());
      }
    }

    apply.close();
  }
}
