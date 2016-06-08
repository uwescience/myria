package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.IntDivideExpression;
import edu.washington.escience.myria.expression.MinusExpression;
import edu.washington.escience.myria.expression.ModuloExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.TimesExpression;
import edu.washington.escience.myria.expression.evaluate.ConstantEvaluator;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

public class ExpressionTest {
  /**
   * Given a constant expression, evaluate it and return its value.
   *
   * @param op the expression
   * @return the constant value
   * @throws DbException if there is an error evaluating the expression
   */
  private Object evaluateConstantExpression(final ExpressionOperator op) throws DbException {
    Expression expr = new Expression("op", op);
    ConstantEvaluator eval =
        new ConstantEvaluator(expr, new ExpressionOperatorParameter(Schema.EMPTY_SCHEMA));
    return eval.eval();
  }

  /**
   * Given a constant expression, try and evaluate it. If successful, return it. If not, unwrap the expression to the
   * root cause and return that.
   *
   * @param op the expression
   * @return the constant value
   * @throws Throwable the root cause of a failed Janino compilation.
   */
  private Object evaluateConstantAndUnrollException(final ExpressionOperator op) throws Throwable {
    try {
      return evaluateConstantExpression(op);
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

  @Test(expected = ArithmeticException.class)
  public void testOverflowIntSum() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Integer.MAX_VALUE);
    ConstantExpression val2 = new ConstantExpression(1);
    ExpressionOperator sum = new PlusExpression(val1, val2);
    evaluateConstantAndUnrollException(sum);
  }

  @Test(expected = ArithmeticException.class)
  public void testUnderflowIntSum() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Integer.MIN_VALUE);
    ConstantExpression val2 = new ConstantExpression(-1);
    ExpressionOperator ans = new PlusExpression(val1, val2);
    evaluateConstantAndUnrollException(ans);
  }

  @Test(expected = ArithmeticException.class)
  public void testOverflowIntDiff() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Integer.MAX_VALUE);
    ConstantExpression val2 = new ConstantExpression(-1);
    ExpressionOperator sum = new MinusExpression(val1, val2);
    evaluateConstantAndUnrollException(sum);
  }

  @Test(expected = ArithmeticException.class)
  public void testUnderflowIntDiff() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Integer.MIN_VALUE);
    ConstantExpression val2 = new ConstantExpression(1);
    ExpressionOperator ans = new MinusExpression(val1, val2);
    evaluateConstantAndUnrollException(ans);
  }

  @Test(expected = ArithmeticException.class)
  public void testOverflowLongSum() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Long.MAX_VALUE);
    ConstantExpression val2 = new ConstantExpression(1L);
    ExpressionOperator ans = new PlusExpression(val1, val2);
    evaluateConstantAndUnrollException(ans);
  }

  @Test(expected = ArithmeticException.class)
  public void testUnderflowLongSum() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Long.MIN_VALUE);
    ConstantExpression val2 = new ConstantExpression(-1L);
    ExpressionOperator ans = new PlusExpression(val1, val2);
    evaluateConstantAndUnrollException(ans);
  }

  @Test(expected = ArithmeticException.class)
  public void testOverflowLongDiff() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Long.MAX_VALUE);
    ConstantExpression val2 = new ConstantExpression(-1L);
    ExpressionOperator ans = new MinusExpression(val1, val2);
    evaluateConstantAndUnrollException(ans);
  }

  @Test(expected = ArithmeticException.class)
  public void testUnderflowLongDiff() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Long.MIN_VALUE);
    ConstantExpression val2 = new ConstantExpression(1L);
    ExpressionOperator ans = new MinusExpression(val1, val2);
    evaluateConstantAndUnrollException(ans);
  }

  @Test(expected = ArithmeticException.class)
  public void testOverflowIntProd() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Integer.MAX_VALUE);
    ConstantExpression val2 = new ConstantExpression(Integer.MAX_VALUE);
    ExpressionOperator ans = new TimesExpression(val1, val2);
    evaluateConstantAndUnrollException(ans);
  }

  @Test(expected = ArithmeticException.class)
  public void testOverflowIntProd2() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Integer.MAX_VALUE);
    ConstantExpression val2 = new ConstantExpression(Integer.MIN_VALUE);
    ExpressionOperator ans = new TimesExpression(val1, val2);
    evaluateConstantAndUnrollException(ans);
  }

  @Test(expected = ArithmeticException.class)
  public void testOverflowLongProd() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Long.MAX_VALUE);
    ConstantExpression val2 = new ConstantExpression(2L);
    ExpressionOperator ans = new TimesExpression(val1, val2);
    evaluateConstantAndUnrollException(ans);
  }

  @Test(expected = ArithmeticException.class)
  public void testOverflowLongProd2() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Long.MAX_VALUE);
    ConstantExpression val2 = new ConstantExpression(Long.MIN_VALUE);
    ExpressionOperator ans = new TimesExpression(val1, val2);
    evaluateConstantAndUnrollException(ans);
  }

  @Test
  public void testIntDivideWithLongs() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(5L);
    ConstantExpression val2 = new ConstantExpression(6L);
    ExpressionOperator expr = new IntDivideExpression(val1, val2);
    Object ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(0L, ans);
    expr = new IntDivideExpression(val2, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(1L, ans);

    ConstantExpression val3 = new ConstantExpression(-5L);
    expr = new IntDivideExpression(val3, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(-1L, ans);
    expr = new IntDivideExpression(val1, val3);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(-1L, ans);
  }

  @Test
  public void testIntDivideWithFloats() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(5f);
    ConstantExpression val2 = new ConstantExpression(6f);
    ExpressionOperator expr = new IntDivideExpression(val1, val2);
    Object ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(0L, ans);
    expr = new IntDivideExpression(val2, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(1L, ans);

    ConstantExpression val3 = new ConstantExpression(-5f);
    expr = new IntDivideExpression(val3, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(-1L, ans);
    expr = new IntDivideExpression(val1, val3);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(-1L, ans);
  }

  @Test
  public void testIntDivideWithInts() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(5);
    ConstantExpression val2 = new ConstantExpression(6);
    ExpressionOperator expr = new IntDivideExpression(val1, val2);
    Object ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Integer.class, ans.getClass());
    assertEquals(0, ans);
    expr = new IntDivideExpression(val2, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Integer.class, ans.getClass());
    assertEquals(1, ans);

    ConstantExpression val3 = new ConstantExpression(-5);
    expr = new IntDivideExpression(val3, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Integer.class, ans.getClass());
    assertEquals(-1, ans);
    expr = new IntDivideExpression(val1, val3);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Integer.class, ans.getClass());
    assertEquals(-1, ans);
  }

  @Test
  public void testIntDivideWithDoubles() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(5.0);
    ConstantExpression val2 = new ConstantExpression(6.0);
    ExpressionOperator expr = new IntDivideExpression(val1, val2);
    Object ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(0L, ans);
    expr = new IntDivideExpression(val2, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(1L, ans);

    ConstantExpression val3 = new ConstantExpression(-5.0);
    expr = new IntDivideExpression(val3, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(-1L, ans);
    expr = new IntDivideExpression(val1, val3);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(-1L, ans);
  }

  @Test
  public void testIntDivideWithDoublesNotOverflow() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(-Math.pow(2, 64));
    ConstantExpression val2 = new ConstantExpression(2.0);
    ExpressionOperator expr = new IntDivideExpression(val1, val2);
    Object ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.MIN_VALUE, ans);
  }

  @Test(expected = ArithmeticException.class)
  public void testIntDivideWithDoublesOverflow() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(Math.pow(2, 63)); /* Long.MAX_VALUE + 1 */
    ConstantExpression val2 = new ConstantExpression(1.0);
    ExpressionOperator expr = new IntDivideExpression(val1, val2);
    evaluateConstantAndUnrollException(expr);
  }

  @Test(expected = ArithmeticException.class)
  public void testIntDivideWithDoublesOverflow2() throws Throwable {
    ConstantExpression val1 =
        new ConstantExpression(Math.pow(2, 62)); /* (Long.MAX_VALUE + 1) >> 1 */
    ConstantExpression val2 = new ConstantExpression(0.5);
    ExpressionOperator expr = new IntDivideExpression(val1, val2);
    evaluateConstantAndUnrollException(expr);
  }

  @Test
  public void testModuloWithLongs() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(5L);
    ConstantExpression val2 = new ConstantExpression(6L);
    ExpressionOperator expr = new ModuloExpression(val1, val2);
    Object ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(5L, ans);
    expr = new ModuloExpression(val2, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(1L, ans);

    ConstantExpression val3 = new ConstantExpression(-5L);
    expr = new ModuloExpression(val3, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(0L, ans);
    expr = new ModuloExpression(val1, val3);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Long.class, ans.getClass());
    assertEquals(0L, ans);
  }

  @Test
  public void testModuloWithInts() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(5);
    ConstantExpression val2 = new ConstantExpression(6);
    ExpressionOperator expr = new ModuloExpression(val1, val2);
    Object ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Integer.class, ans.getClass());
    assertEquals(5, ans);
    expr = new ModuloExpression(val2, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Integer.class, ans.getClass());
    assertEquals(1, ans);

    ConstantExpression val3 = new ConstantExpression(-5);
    expr = new ModuloExpression(val3, val1);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Integer.class, ans.getClass());
    assertEquals(0, ans);
    expr = new ModuloExpression(val1, val3);
    ans = evaluateConstantAndUnrollException(expr);
    assertEquals(Integer.class, ans.getClass());
    assertEquals(0, ans);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testModuloWithFloats() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(5.0F);
    ConstantExpression val2 = new ConstantExpression(6.0F);
    ExpressionOperator expr = new ModuloExpression(val1, val2);
    evaluateConstantAndUnrollException(expr);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testModuloWithDoubles() throws Throwable {
    ConstantExpression val1 = new ConstantExpression(5.0);
    ConstantExpression val2 = new ConstantExpression(6.0);
    ExpressionOperator expr = new ModuloExpression(val1, val2);
    evaluateConstantAndUnrollException(expr);
  }

  @Test
  public void testLongString() throws Throwable {
    ConstantExpression tenBillion = new ConstantExpression(Type.LONG_TYPE, "10000000000");
    assertEquals(10000000000L, evaluateConstantAndUnrollException(tenBillion));
    tenBillion = new ConstantExpression(Type.LONG_TYPE, "10000000000l");
    assertEquals(10000000000L, evaluateConstantAndUnrollException(tenBillion));
    tenBillion = new ConstantExpression(Type.LONG_TYPE, "10000000000L");
    assertEquals(10000000000L, evaluateConstantAndUnrollException(tenBillion));
  }

  @Test
  public void testFloatString() throws Throwable {
    ConstantExpression piApprox = new ConstantExpression(Type.FLOAT_TYPE, "3.1415926");
    assertEquals((float) Math.PI, (Float) evaluateConstantAndUnrollException(piApprox), 0.00001);
    piApprox = new ConstantExpression(Type.FLOAT_TYPE, "3.1415926f");
    assertEquals((float) Math.PI, (Float) evaluateConstantAndUnrollException(piApprox), 0.00001);
    piApprox = new ConstantExpression(Type.FLOAT_TYPE, "3.1415926F");
    assertEquals((float) Math.PI, (Float) evaluateConstantAndUnrollException(piApprox), 0.00001);
  }

  @Test
  public void testIntString() throws Throwable {
    ConstantExpression oneBillion = new ConstantExpression(Type.INT_TYPE, "1000000000");
    assertEquals(1000000000, evaluateConstantAndUnrollException(oneBillion));
  }

  @Test
  public void testDoubleString() throws Throwable {
    ConstantExpression piApprox = new ConstantExpression(Type.DOUBLE_TYPE, String.valueOf(Math.PI));
    assertEquals(Math.PI, (Double) evaluateConstantAndUnrollException(piApprox), 0.00000001);
  }
}
