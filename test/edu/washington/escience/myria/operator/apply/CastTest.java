package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;

import org.joda.time.DateTime;
import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.CastExpression;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.TypeExpression;
import edu.washington.escience.myria.expression.evaluate.ConstantEvaluator;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

public class CastTest {

  /**
   * Given a cast expression, evaluate it and return its value.
   *
   * @param op the expression
   * @return the constant value
   * @throws DbException if there is an error evaluating the expression
   */
  private Object evaluateCastExpression(ExpressionOperator op, Type type) throws DbException {
    Expression expr = new Expression("op", new CastExpression(op, new TypeExpression(type)));
    ConstantEvaluator eval =
        new ConstantEvaluator(expr, new ExpressionOperatorParameter(Schema.EMPTY_SCHEMA));
    return eval.eval();
  }

  /**
   * Given a cast expression, try and evaluate it. If successful, return it. If not, unwrap the expression to the root
   * cause and return that.
   *
   * @param op the expression
   * @return the constant value
   * @throws Throwable the root cause of a failed Janino compilation.
   */
  private Object evaluateCastAndUnrollException(ExpressionOperator op, Type type) throws Throwable {
    try {
      Object ans = evaluateCastExpression(op, type);
      assertType(ans, type);
      return ans;
    } catch (DbException e) {
      Throwable e1 = e.getCause();
      if (e1 == null || !(e1 instanceof InvocationTargetException)) {
        throw e;
      }
      Throwable e2 = e1.getCause();
      if (e2 == null) {
        throw e;
      }
      throw e2;
    }
  }

  private void assertType(Object obj, Type type) throws DbException {
    switch (type) {
      case INT_TYPE:
        assertEquals(obj.getClass(), Integer.class);
        break;
      case FLOAT_TYPE:
        assertEquals(obj.getClass(), Float.class);
        break;
      case LONG_TYPE:
        assertEquals(obj.getClass(), Long.class);
        break;
      case DOUBLE_TYPE:
        assertEquals(obj.getClass(), Double.class);
        break;
      case STRING_TYPE:
        assertEquals(obj.getClass(), String.class);
        break;
      case DATETIME_TYPE:
        assertEquals(obj.getClass(), DateTime.class);
        break;
      case BOOLEAN_TYPE:
        assertEquals(obj.getClass(), Boolean.class);
        break;
    }
  }

  @Test
  public void testLongToInt() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(0L);
    Object ans = evaluateCastAndUnrollException(val1, Type.INT_TYPE);
    assertEquals(0, ans);
    ConstantExpression val2 = new ConstantExpression((long) Integer.MAX_VALUE);
    ans = evaluateCastAndUnrollException(val2, Type.INT_TYPE);
    assertEquals(Integer.MAX_VALUE, ans);
    ConstantExpression val3 = new ConstantExpression((long) Integer.MIN_VALUE);
    ans = evaluateCastAndUnrollException(val3, Type.INT_TYPE);
    assertEquals(Integer.MIN_VALUE, ans);
  }

  @Test
  public void testFloatToInt() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(0.0f);
    Object ans = evaluateCastAndUnrollException(val1, Type.INT_TYPE);
    assertEquals(0, ans);
    // cannot use Integer.MAX_VALUE here since converting (float) Integer.MAX_VALUE back to int will overflow.
    float maxIntFloat = (float) 2.147483E9;
    ConstantExpression val2 = new ConstantExpression(maxIntFloat);
    ans = evaluateCastAndUnrollException(val2, Type.INT_TYPE);
    assertEquals((int) maxIntFloat, ((Integer) ans).intValue());
    float minIntFloat = Integer.MIN_VALUE;
    ConstantExpression val3 = new ConstantExpression(minIntFloat);
    ans = evaluateCastAndUnrollException(val3, Type.INT_TYPE);
    assertEquals((int) minIntFloat, ans);
    ConstantExpression val4 = new ConstantExpression((float) 3.25736);
    ans = evaluateCastAndUnrollException(val4, Type.INT_TYPE);
    assertEquals(3, ans);
  }

  @Test
  public void testDoubleToInt() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(0.0);
    Object ans = evaluateCastAndUnrollException(val1, Type.INT_TYPE);
    assertEquals(0, ans);
    double maxIntDouble = Integer.MAX_VALUE;
    ConstantExpression val2 = new ConstantExpression(maxIntDouble);
    ans = evaluateCastAndUnrollException(val2, Type.INT_TYPE);
    assertEquals((int) maxIntDouble, ((Integer) ans).intValue());
    double minIntDouble = Integer.MIN_VALUE;
    ConstantExpression val3 = new ConstantExpression(minIntDouble);
    ans = evaluateCastAndUnrollException(val3, Type.INT_TYPE);
    assertEquals((int) minIntDouble, ans);
    ConstantExpression val4 = new ConstantExpression((float) 3.25736);
    ans = evaluateCastAndUnrollException(val4, Type.INT_TYPE);
    assertEquals(3, ans);
  }

  @Test
  public void testIntToFloat() throws Throwable {
    float tolerance = (float) 1e-6;
    ConstantExpression val1 = new ConstantExpression(0);
    Object ans = evaluateCastAndUnrollException(val1, Type.FLOAT_TYPE);
    assertEquals(Float.class, ans.getClass());
    assertEquals(0.0f, ((Float) ans).floatValue(), tolerance);
    ConstantExpression val2 = new ConstantExpression(Integer.MAX_VALUE);
    ans = evaluateCastAndUnrollException(val2, Type.FLOAT_TYPE);
    assertEquals(Float.class, ans.getClass());
    assertEquals(Integer.MAX_VALUE, ((Float) ans).floatValue(), tolerance);
    ConstantExpression val3 = new ConstantExpression(Integer.MIN_VALUE);
    ans = evaluateCastAndUnrollException(val3, Type.FLOAT_TYPE);
    assertEquals(Float.class, ans.getClass());
    assertEquals(Integer.MIN_VALUE, ((Float) ans).floatValue(), tolerance);
  }

  @Test
  public void testLongToFloat() throws Throwable {
    float tolerance = (float) 1e-6;
    ConstantExpression val1 = new ConstantExpression(0L);
    Object ans = evaluateCastAndUnrollException(val1, Type.FLOAT_TYPE);
    assertEquals(0.0f, ((Float) ans).floatValue(), tolerance);
    ConstantExpression val2 = new ConstantExpression(Long.MAX_VALUE);
    ans = evaluateCastAndUnrollException(val2, Type.FLOAT_TYPE);
    assertEquals(Long.MAX_VALUE, ((Float) ans).floatValue(), tolerance);
    ConstantExpression val3 = new ConstantExpression(Long.MIN_VALUE);
    ans = evaluateCastAndUnrollException(val3, Type.FLOAT_TYPE);
    assertEquals(Long.MIN_VALUE, ((Float) ans).floatValue(), tolerance);
  }

  @Test
  public void testDoubleToFloat() throws Throwable {
    float tolerance = (float) 1e-6;
    ConstantExpression val1 = new ConstantExpression(0.0);
    Object ans = evaluateCastAndUnrollException(val1, Type.FLOAT_TYPE);
    assertEquals(0.0f, ((Float) ans).floatValue(), tolerance);
    ConstantExpression val2 = new ConstantExpression((double) Float.MAX_VALUE);
    ans = evaluateCastAndUnrollException(val2, Type.FLOAT_TYPE);
    assertEquals(Float.MAX_VALUE, ((Float) ans).floatValue(), tolerance);
    ConstantExpression val3 = new ConstantExpression((double) -Float.MAX_VALUE);
    ans = evaluateCastAndUnrollException(val3, Type.FLOAT_TYPE);
    assertEquals(-Float.MAX_VALUE, ((Float) ans).floatValue(), tolerance);
  }

  @Test
  public void testIntToDouble() throws Throwable {
    double tolerance = 1e-8;
    ConstantExpression val1 = new ConstantExpression(0);
    Object ans = evaluateCastAndUnrollException(val1, Type.DOUBLE_TYPE);
    assertEquals(0.0f, ((Double) ans).doubleValue(), tolerance);
    ConstantExpression val2 = new ConstantExpression(Integer.MAX_VALUE);
    ans = evaluateCastAndUnrollException(val2, Type.DOUBLE_TYPE);
    assertEquals(Integer.MAX_VALUE, ((Double) ans).doubleValue(), tolerance);
    ConstantExpression val3 = new ConstantExpression(Integer.MIN_VALUE);
    ans = evaluateCastAndUnrollException(val3, Type.DOUBLE_TYPE);
    assertEquals(Integer.MIN_VALUE, ((Double) ans).doubleValue(), tolerance);
  }

  @Test
  public void testFloatToDouble() throws Throwable {
    double tolerance = 1e-8;
    ConstantExpression val1 = new ConstantExpression(0.0f);
    Object ans = evaluateCastAndUnrollException(val1, Type.DOUBLE_TYPE);
    assertEquals(0.0f, ((Double) ans).doubleValue(), tolerance);
    ConstantExpression val2 = new ConstantExpression(Float.MAX_VALUE);
    ans = evaluateCastAndUnrollException(val2, Type.DOUBLE_TYPE);
    assertEquals(Float.MAX_VALUE, ((Double) ans).doubleValue(), tolerance);
    ConstantExpression val3 = new ConstantExpression(-Float.MAX_VALUE);
    ans = evaluateCastAndUnrollException(val3, Type.DOUBLE_TYPE);
    assertEquals(-Float.MAX_VALUE, ((Double) ans).doubleValue(), tolerance);
  }

  @Test
  public void testLongToDouble() throws Throwable {
    double tolerance = 1e-8;
    ConstantExpression val1 = new ConstantExpression(0L);
    Object ans = evaluateCastAndUnrollException(val1, Type.DOUBLE_TYPE);
    assertEquals(0.0f, ((Double) ans).doubleValue(), tolerance);
    ConstantExpression val2 = new ConstantExpression(Long.MAX_VALUE);
    ans = evaluateCastAndUnrollException(val2, Type.DOUBLE_TYPE);
    assertEquals(Long.MAX_VALUE, ((Double) ans).doubleValue(), tolerance);
    ConstantExpression val3 = new ConstantExpression(Long.MIN_VALUE);
    ans = evaluateCastAndUnrollException(val3, Type.DOUBLE_TYPE);
    assertEquals(Long.MIN_VALUE, ((Double) ans).doubleValue(), tolerance);
  }

  @Test
  public void testToString() throws Throwable {
    int test1 = Integer.MAX_VALUE;
    ConstantExpression val1 = new ConstantExpression(test1);
    Object ans = evaluateCastAndUnrollException(val1, Type.STRING_TYPE);
    assertEquals(String.valueOf(test1), ans);
    int test2 = Integer.MIN_VALUE;
    ConstantExpression val2 = new ConstantExpression(test2);
    ans = evaluateCastAndUnrollException(val2, Type.STRING_TYPE);
    assertEquals(String.valueOf(test2), ans);
    long test3 = Long.MAX_VALUE;
    ConstantExpression val3 = new ConstantExpression(test3);
    ans = evaluateCastAndUnrollException(val3, Type.STRING_TYPE);
    assertEquals(String.valueOf(test3), ans);
    long test4 = Long.MIN_VALUE;
    ConstantExpression val4 = new ConstantExpression(test4);
    ans = evaluateCastAndUnrollException(val4, Type.STRING_TYPE);
    assertEquals(String.valueOf(test4), ans);
    float test5 = (float) 12.6734;
    ConstantExpression val5 = new ConstantExpression(test5);
    ans = evaluateCastAndUnrollException(val5, Type.STRING_TYPE);
    assertEquals(String.valueOf(test5), ans);
    double test6 = 12.6734;
    ConstantExpression val6 = new ConstantExpression(test6);
    ans = evaluateCastAndUnrollException(val6, Type.STRING_TYPE);
    assertEquals(String.valueOf(test6), ans);
  }

  @Test
  public void testStringToNumeric() throws Throwable {
    int test1 = Integer.MAX_VALUE;
    ConstantExpression val1 = new ConstantExpression(String.valueOf(test1));
    Object ans = evaluateCastAndUnrollException(val1, Type.INT_TYPE);
    assertEquals(Integer.MAX_VALUE, ans);
    int test2 = Integer.MIN_VALUE;
    ConstantExpression val2 = new ConstantExpression(String.valueOf(test2));
    ans = evaluateCastAndUnrollException(val2, Type.INT_TYPE);
    assertEquals(test2, ans);
    long test3 = Long.MAX_VALUE;
    ConstantExpression val3 = new ConstantExpression(String.valueOf(test3));
    ans = evaluateCastAndUnrollException(val3, Type.LONG_TYPE);
    assertEquals(test3, ans);
    long test4 = Long.MIN_VALUE;
    ConstantExpression val4 = new ConstantExpression(String.valueOf(test4));
    ans = evaluateCastAndUnrollException(val4, Type.LONG_TYPE);
    assertEquals(test4, ans);
    float test5 = (float) 12.6734;
    ConstantExpression val5 = new ConstantExpression(String.valueOf(test5));
    ans = evaluateCastAndUnrollException(val5, Type.FLOAT_TYPE);
    assertEquals(test5, ans);
    double test6 = 12.6734;
    ConstantExpression val6 = new ConstantExpression(String.valueOf(test6));
    ans = evaluateCastAndUnrollException(val6, Type.DOUBLE_TYPE);
    assertEquals(test6, ans);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLongToIntOverflow() throws Throwable {
    ConstantExpression val = new ConstantExpression((long) Integer.MAX_VALUE + 1);
    evaluateCastAndUnrollException(val, Type.INT_TYPE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLongToIntUnderflow() throws Throwable {
    ConstantExpression val = new ConstantExpression((long) Integer.MIN_VALUE - 1);
    evaluateCastAndUnrollException(val, Type.INT_TYPE);
  }

  @Test(expected = ArithmeticException.class)
  public void testFloatToIntOverflow() throws Throwable {
    ConstantExpression val = new ConstantExpression(Float.MAX_VALUE);
    evaluateCastAndUnrollException(val, Type.INT_TYPE);
  }

  public void testFloatToIntUnderflow() throws Throwable {
    ConstantExpression val = new ConstantExpression(-Float.MAX_VALUE);
    evaluateCastAndUnrollException(val, Type.INT_TYPE);
  }

  @Test(expected = ArithmeticException.class)
  public void testDoubleToIntOverflow() throws Throwable {
    ConstantExpression val = new ConstantExpression(Double.MAX_VALUE);
    evaluateCastAndUnrollException(val, Type.INT_TYPE);
  }

  @Test(expected = ArithmeticException.class)
  public void testDoubleToIntUnderflow() throws Throwable {
    ConstantExpression val = new ConstantExpression(-Double.MAX_VALUE);
    evaluateCastAndUnrollException(val, Type.INT_TYPE);
  }

  @Test(expected = ArithmeticException.class)
  public void testFloatToLongOverflow() throws Throwable {
    ConstantExpression val = new ConstantExpression(Float.MAX_VALUE);
    evaluateCastAndUnrollException(val, Type.LONG_TYPE);
  }

  @Test(expected = ArithmeticException.class)
  public void testFloatToLongUnderflow() throws Throwable {
    ConstantExpression val = new ConstantExpression(-Float.MAX_VALUE);
    evaluateCastAndUnrollException(val, Type.LONG_TYPE);
  }

  @Test(expected = ArithmeticException.class)
  public void testDoubleToLongOverflow() throws Throwable {
    ConstantExpression val = new ConstantExpression(Double.MAX_VALUE);
    evaluateCastAndUnrollException(val, Type.LONG_TYPE);
  }

  @Test(expected = ArithmeticException.class)
  public void testDoubleToLongUnderflow() throws Throwable {
    ConstantExpression val = new ConstantExpression(-Double.MAX_VALUE);
    evaluateCastAndUnrollException(val, Type.LONG_TYPE);
  }

  @Test(expected = NumberFormatException.class)
  public void testStringToIntFormatError() throws Throwable {
    ConstantExpression val = new ConstantExpression("12.95");
    evaluateCastAndUnrollException(val, Type.INT_TYPE);
  }

  @Test(expected = NumberFormatException.class)
  public void testStringToFloatFormatError() throws Throwable {
    ConstantExpression val = new ConstantExpression("12.95abc");
    evaluateCastAndUnrollException(val, Type.FLOAT_TYPE);
  }

  @Test(expected = NumberFormatException.class)
  public void testStringToDoubleFormatError() throws Throwable {
    ConstantExpression val = new ConstantExpression("12.95abc");
    evaluateCastAndUnrollException(val, Type.DOUBLE_TYPE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void unsupportedCast() throws IllegalArgumentException {
    ExpressionOperator cast =
        new CastExpression(new ConstantExpression(12), new TypeExpression(Type.DATETIME_TYPE));
    cast.getOutputType(new ExpressionOperatorParameter());
  }

  @Test
  public void supportedNoopCast() throws IllegalArgumentException {
    ExpressionOperator cast =
        new CastExpression(new ConstantExpression(12L), new TypeExpression(Type.LONG_TYPE));
    assertEquals(Type.LONG_TYPE, cast.getOutputType(new ExpressionOperatorParameter()));
  }

  /* Self-cast tests. */
  @Test
  public void testSelfCastBoolean() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(true);
    Object ans = evaluateCastAndUnrollException(val1, Type.BOOLEAN_TYPE);
    assertEquals(true, ans);
  }

  @Test
  public void testSelfCastDouble() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(2.0);
    Object ans = evaluateCastAndUnrollException(val1, Type.DOUBLE_TYPE);
    assertEquals(2.0, ans);
  }

  @Test
  public void testSelfCastFloat() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(2.0f);
    Object ans = evaluateCastAndUnrollException(val1, Type.FLOAT_TYPE);
    assertEquals(2.0f, ans);
  }

  @Test
  public void testSelfCastInt() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Integer.MAX_VALUE);
    Object ans = evaluateCastAndUnrollException(val1, Type.INT_TYPE);
    assertEquals(Integer.MAX_VALUE, ans);
  }

  @Test
  public void testSelfCastLong() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Long.MAX_VALUE);
    Object ans = evaluateCastAndUnrollException(val1, Type.LONG_TYPE);
    assertEquals(Long.MAX_VALUE, ans);
  }

  @Test
  public void testSelfCastString() throws Throwable {
    ConstantExpression val1 = new ConstantExpression("abc123");
    Object ans = evaluateCastAndUnrollException(val1, Type.STRING_TYPE);
    assertEquals("abc123", ans);
  }
}
