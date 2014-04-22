package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.CastExpression;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.TypeExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class CastTest {

  @Test
  public void testLegalNumericCast() throws DbException {
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.INT_TYPE, Type.LONG_TYPE, Type.FLOAT_TYPE, Type.DOUBLE_TYPE, Type.STRING_TYPE),
            ImmutableList.of("int", "long", "float", "double", "str"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    int[] testCases = { 0, -1, 1, Integer.MIN_VALUE, Integer.MAX_VALUE - 1 };
    for (int testCase : testCases) {
      tbb.putInt(0, testCase);
      tbb.putLong(1, testCase);
      tbb.putFloat(2, testCase);
      tbb.putDouble(3, testCase);
      tbb.putString(4, String.valueOf(testCase));
    }
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();

    ExpressionOperator vara = new VariableExpression(0);
    ExpressionOperator varb = new VariableExpression(1);
    ExpressionOperator varc = new VariableExpression(2);
    ExpressionOperator vard = new VariableExpression(3);
    ExpressionOperator vare = new VariableExpression(4);
    {
      // cast int to long.
      Expression expr = new Expression("intToLong", new CastExpression(vara, new TypeExpression(Type.LONG_TYPE)));
      Expressions.add(expr);
    }
    {
      // cast long to int.
      Expression expr = new Expression("LongToInt", new CastExpression(varb, new TypeExpression(Type.INT_TYPE)));
      Expressions.add(expr);
    }

    {
      // cast double to float.
      Expression expr = new Expression("doubleToFloat", new CastExpression(vard, new TypeExpression(Type.FLOAT_TYPE)));
      Expressions.add(expr);
    }
    {
      // cast float to double
      Expression expr = new Expression("FloatToDouble", new CastExpression(varc, new TypeExpression(Type.DOUBLE_TYPE)));
      Expressions.add(expr);
    }
    {
      // cast int to string.
      Expression expr = new Expression("intToStr", new CastExpression(vara, new TypeExpression(Type.STRING_TYPE)));
      Expressions.add(expr);
    }
    {
      // cast long to string.
      Expression expr = new Expression("longToStr", new CastExpression(varb, new TypeExpression(Type.STRING_TYPE)));
      Expressions.add(expr);
    }
    {
      // cast float to string.
      Expression expr = new Expression("floatToStr", new CastExpression(varc, new TypeExpression(Type.STRING_TYPE)));
      Expressions.add(expr);
    }
    {
      // cast double to string.
      Expression expr = new Expression("doubleToStr", new CastExpression(vard, new TypeExpression(Type.STRING_TYPE)));
      Expressions.add(expr);
    }
    {
      // cast string to int.
      Expression expr = new Expression("strToInt", new CastExpression(vare, new TypeExpression(Type.INT_TYPE)));
      Expressions.add(expr);
    }
    {
      // cast string to long.
      Expression expr = new Expression("strToLong", new CastExpression(vare, new TypeExpression(Type.LONG_TYPE)));
      Expressions.add(expr);
    }
    {
      // cast string to float.
      Expression expr = new Expression("strToFloat", new CastExpression(vare, new TypeExpression(Type.FLOAT_TYPE)));
      Expressions.add(expr);
    }
    {
      // cast string to double.
      Expression expr = new Expression("strToDouble", new CastExpression(vare, new TypeExpression(Type.DOUBLE_TYPE)));
      Expressions.add(expr);
    }
    Apply apply = new Apply(new TupleSource(tbb), Expressions.build());

    apply.open(TestEnvVars.get());
    TupleBatch result;
    int resultSize = 0;
    double doubleTolerance = 0.0000001;
    float floatTolerance = (float) 0.00001;
    while (!apply.eos()) {
      result = apply.nextReady();
      if (result != null) {
        assertEquals(12, result.getSchema().numColumns());
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(0));
        assertEquals(Type.INT_TYPE, result.getSchema().getColumnType(1));
        assertEquals(Type.FLOAT_TYPE, result.getSchema().getColumnType(2));
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(3));
        assertEquals(Type.STRING_TYPE, result.getSchema().getColumnType(4));
        assertEquals(Type.STRING_TYPE, result.getSchema().getColumnType(5));
        assertEquals(Type.STRING_TYPE, result.getSchema().getColumnType(6));
        assertEquals(Type.STRING_TYPE, result.getSchema().getColumnType(7));
        assertEquals(Type.INT_TYPE, result.getSchema().getColumnType(8));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(9));
        assertEquals(Type.FLOAT_TYPE, result.getSchema().getColumnType(10));
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(11));

        for (int row = 0; row < result.numTuples(); row++) {
          assertEquals(testCases[row], result.getLong(0, row));
          assertEquals(testCases[row], result.getInt(1, row));
          double d = testCases[row];
          assertEquals((float) d, result.getFloat(2, row), floatTolerance);
          float f = testCases[row];
          assertEquals(f, result.getDouble(3, row), doubleTolerance);
          assertEquals(String.valueOf(testCases[row]), result.getString(4, row));
          assertEquals(String.valueOf(testCases[row]), result.getString(5, row));
          assertEquals(String.valueOf((float) testCases[row]), result.getString(6, row));
          assertEquals(String.valueOf((double) testCases[row]), result.getString(7, row));
          assertEquals(testCases[row], result.getInt(8, row));
          assertEquals(testCases[row], result.getLong(9, row));
          assertEquals(testCases[row], result.getFloat(10, row), floatTolerance);
          assertEquals(testCases[row], result.getDouble(11, row), doubleTolerance);
        }
        resultSize += result.numTuples();
      }
    }
    assertEquals(testCases.length, resultSize);
    apply.close();

  }

  @Test(expected = IllegalArgumentException.class)
  public void testLongToIntOutOfRange() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("long"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    tbb.putLong(0, ((long) Integer.MAX_VALUE) + 1);
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    Expression expr =
        new Expression("LongToInt", new CastExpression(new VariableExpression(0), new TypeExpression(Type.INT_TYPE)));
    Expressions.add(expr);
    Apply apply = new Apply(new TupleSource(tbb), Expressions.build());
    apply.open(TestEnvVars.get());
    apply.nextReady();
    apply.close();
  }

  @Test(expected = ArithmeticException.class)
  public void testFloatToIntOutOfRange() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.FLOAT_TYPE), ImmutableList.of("float"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    tbb.putFloat(0, Float.MAX_VALUE);
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    Expression expr =
        new Expression("FloatToInt", new CastExpression(new VariableExpression(0), new TypeExpression(Type.INT_TYPE)));
    Expressions.add(expr);
    Apply apply = new Apply(new TupleSource(tbb), Expressions.build());
    apply.open(TestEnvVars.get());
    apply.nextReady();
    apply.close();
  }

  @Test(expected = ArithmeticException.class)
  public void testDoubleToIntOutOfRange() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.DOUBLE_TYPE), ImmutableList.of("double"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    tbb.putDouble(0, Double.MAX_VALUE);
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    Expression expr =
        new Expression("DoubleToInt", new CastExpression(new VariableExpression(0), new TypeExpression(Type.INT_TYPE)));
    Expressions.add(expr);
    Apply apply = new Apply(new TupleSource(tbb), Expressions.build());
    apply.open(TestEnvVars.get());
    apply.nextReady();
    apply.close();
  }

  @Test(expected = ArithmeticException.class)
  public void testFloatToLongOutOfRange() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.FLOAT_TYPE), ImmutableList.of("float"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    tbb.putFloat(0, Float.MAX_VALUE);
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    Expression expr =
        new Expression("FloatToLong", new CastExpression(new VariableExpression(0), new TypeExpression(Type.LONG_TYPE)));
    Expressions.add(expr);
    Apply apply = new Apply(new TupleSource(tbb), Expressions.build());
    apply.open(TestEnvVars.get());
    apply.nextReady();
    apply.close();
  }

  @Test(expected = ArithmeticException.class)
  public void testDoubleToLongOutOfRange() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.DOUBLE_TYPE), ImmutableList.of("double"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    tbb.putDouble(0, Double.MAX_VALUE);
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    Expression expr =
        new Expression("DoubleToLong",
            new CastExpression(new VariableExpression(0), new TypeExpression(Type.LONG_TYPE)));
    Expressions.add(expr);
    Apply apply = new Apply(new TupleSource(tbb), Expressions.build());
    apply.open(TestEnvVars.get());
    apply.nextReady();
    apply.close();
  }

  @Test(expected = NumberFormatException.class)
  public void testStringToIntIllegal() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.STRING_TYPE), ImmutableList.of("string"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    tbb.putString(0, "12.35");
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    Expression expr =
        new Expression("StringToInt", new CastExpression(new VariableExpression(0), new TypeExpression(Type.INT_TYPE)));
    Expressions.add(expr);
    Apply apply = new Apply(new TupleSource(tbb), Expressions.build());
    apply.open(TestEnvVars.get());
    apply.nextReady();
    apply.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void unsupportedCast() throws IllegalArgumentException {
    ExpressionOperator cast = new CastExpression(new ConstantExpression(12), new TypeExpression(Type.DATETIME_TYPE));
    cast.getOutputType(new ExpressionOperatorParameter());
  }

}
