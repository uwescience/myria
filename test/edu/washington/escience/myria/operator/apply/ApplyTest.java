package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.AbsExpression;
import edu.washington.escience.myria.expression.AndExpression;
import edu.washington.escience.myria.expression.CeilExpression;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.CosExpression;
import edu.washington.escience.myria.expression.DivideExpression;
import edu.washington.escience.myria.expression.EqualsExpression;
import edu.washington.escience.myria.expression.ObjectExpression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.FloorExpression;
import edu.washington.escience.myria.expression.GreaterThanExpression;
import edu.washington.escience.myria.expression.GreaterThanOrEqualsExpression;
import edu.washington.escience.myria.expression.LessThanExpression;
import edu.washington.escience.myria.expression.LessThanOrEqualsExpression;
import edu.washington.escience.myria.expression.MinusExpression;
import edu.washington.escience.myria.expression.NotEqualsExpression;
import edu.washington.escience.myria.expression.NotExpression;
import edu.washington.escience.myria.expression.OrExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.PowExpression;
import edu.washington.escience.myria.expression.SinExpression;
import edu.washington.escience.myria.expression.SqrtExpression;
import edu.washington.escience.myria.expression.TanExpression;
import edu.washington.escience.myria.expression.TimesExpression;
import edu.washington.escience.myria.expression.ToUpperCaseExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.TupleSource;

public class ApplyTest {

  private final int NUM_TUPLES = 2 * TupleBatch.BATCH_SIZE;
  private final int SMALL_NUM_TUPLES = 10;

  @Test
  public void testApply() throws DbException {
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE, Type.STRING_TYPE, Type.BOOLEAN_TYPE),
            ImmutableList.of("a", "b", "c", "d", "e"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < NUM_TUPLES; i++) {
      tbb.putLong(0, (long) Math.pow(i, 2));
      tbb.putLong(1, i + 1);
      tbb.putInt(2, (int) i);
      tbb.putString(3, "Foo" + i);
      tbb.putBoolean(4, i % 2 == 0);
    }
    ImmutableList.Builder<ObjectExpression> objectExpressions = ImmutableList.builder();

    ExpressionOperator vara = new VariableExpression(0);
    ExpressionOperator varb = new VariableExpression(1);
    ExpressionOperator varc = new VariableExpression(2);
    ExpressionOperator vard = new VariableExpression(3);
    ExpressionOperator vare = new VariableExpression(4);

    {
      // Expression: Math.sqrt(a);

      ExpressionOperator squareRoot = new SqrtExpression(vara);

      ObjectExpression expr = new ObjectExpression("first", squareRoot);

      objectExpressions.add(expr);
    }

    {
      // Expression: (b+c) * (b-c)

      ExpressionOperator plus = new PlusExpression(varb, varc);
      ExpressionOperator minus = new MinusExpression(varb, varc);

      ExpressionOperator times = new TimesExpression(plus, minus);

      ObjectExpression expr = new ObjectExpression("second", times);
      objectExpressions.add(expr);
    }

    {
      // Expression: Math.sqrt(Math.pow(a, 2) + Math.pow(b, 2))

      ExpressionOperator two = new ConstantExpression(Type.INT_TYPE, "2");
      ExpressionOperator pow1 = new PowExpression(vara, two);
      ExpressionOperator pow2 = new PowExpression(varb, two);

      ExpressionOperator plus = new PlusExpression(pow1, pow2);

      ExpressionOperator sqrt = new SqrtExpression(plus);

      ObjectExpression expr = new ObjectExpression("third", sqrt);
      objectExpressions.add(expr);
    }

    {
      // Expression: d.toUpperCase()

      ExpressionOperator upper = new ToUpperCaseExpression(vard);

      ObjectExpression expr = new ObjectExpression("fourth", upper);
      objectExpressions.add(expr);
    }

    {
      // Expression: Math.abs(b-a)

      ExpressionOperator abs = new AbsExpression(new MinusExpression(varb, vara));

      ObjectExpression expr = new ObjectExpression("fifth", abs);
      objectExpressions.add(expr);
    }

    {
      // Expression: Math.floor(Math.sqrt(a)) + Math.ceil(Math.sqrt(a));

      ExpressionOperator squareRoot = new SqrtExpression(vara);
      ExpressionOperator floor = new FloorExpression(squareRoot);
      ExpressionOperator ceil = new CeilExpression(squareRoot);
      ExpressionOperator plus = new PlusExpression(floor, ceil);

      ObjectExpression expr = new ObjectExpression("sixth", plus);

      objectExpressions.add(expr);
    }

    {
      // Expression: Math.cos(a * Math.PI / 180) * 2 + Math.sin(a * Math.PI / 180) * 3 + Math.tan(a * Math.PI / 180) *
      // 4;

      ExpressionOperator angle =
          new DivideExpression(new TimesExpression(vara, new ConstantExpression(Type.DOUBLE_TYPE, "Math.PI")),
              new ConstantExpression(Type.INT_TYPE, "180"));
      ExpressionOperator cos =
          new TimesExpression(new CosExpression(angle), new ConstantExpression(Type.INT_TYPE, "2"));
      ExpressionOperator sin =
          new TimesExpression(new SinExpression(angle), new ConstantExpression(Type.INT_TYPE, "3"));
      ExpressionOperator tan =
          new TimesExpression(new TanExpression(angle), new ConstantExpression(Type.INT_TYPE, "4"));
      ExpressionOperator add = new PlusExpression(new PlusExpression(cos, sin), tan);

      ObjectExpression expr = new ObjectExpression("trig", add);

      objectExpressions.add(expr);
    }

    {
      // Expression: !(false || e && true);

      ExpressionOperator and = new AndExpression(vare, new ConstantExpression(Type.BOOLEAN_TYPE, "true"));
      ExpressionOperator or = new OrExpression(new ConstantExpression(Type.BOOLEAN_TYPE, "false"), and);
      ExpressionOperator not = new NotExpression(or);
      ObjectExpression expr = new ObjectExpression("boolean", not);

      objectExpressions.add(expr);
    }

    {
      // Expression (just copy/ rename): a;
      ObjectExpression expr = new ObjectExpression("copy", vara);

      assertTrue(!expr.needsCompiling());
      objectExpressions.add(expr);
    }

    Apply apply = new Apply(new TupleSource(tbb), objectExpressions.build());

    apply.open(null);
    TupleBatch result;
    int resultSize = 0;
    final double tolerance = 0.0000001;
    while (!apply.eos()) {
      result = apply.nextReady();
      if (result != null) {
        assertEquals(9, result.getSchema().numColumns());
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(0));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(1));
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(2));
        assertEquals(Type.STRING_TYPE, result.getSchema().getColumnType(3));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(4));
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(5));
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(6));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(7));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(8));

        assertEquals("first", result.getSchema().getColumnName(0));
        assertEquals("second", result.getSchema().getColumnName(1));
        assertEquals("third", result.getSchema().getColumnName(2));
        assertEquals("fourth", result.getSchema().getColumnName(3));
        assertEquals("fifth", result.getSchema().getColumnName(4));
        assertEquals("sixth", result.getSchema().getColumnName(5));
        assertEquals("trig", result.getSchema().getColumnName(6));
        assertEquals("boolean", result.getSchema().getColumnName(7));
        assertEquals("copy", result.getSchema().getColumnName(8));

        for (int curI = 0; curI < result.numTuples(); curI++) {
          long i = curI + resultSize;
          long a = (long) Math.pow(i, 2);
          long b = i + 1;
          int c = (int) i;
          String d = ("Foo" + i).toUpperCase();
          boolean e = i % 2 == 0;
          assertEquals(i, result.getDouble(0, curI), tolerance);
          assertEquals((b + c) * (b - c), result.getLong(1, curI));
          assertEquals(Math.sqrt(Math.pow(a, 2) + Math.pow(b, 2)), result.getDouble(2, curI), tolerance);
          assertEquals(d, result.getString(3, curI));
          assertEquals(Math.abs(b - a), result.getLong(4, curI));
          assertEquals(Math.floor(Math.sqrt(a)) + Math.ceil(Math.sqrt(a)), result.getDouble(5, curI), tolerance);
          assertEquals(Math.cos(a * Math.PI / 180) * 2 + Math.sin(a * Math.PI / 180) * 3 + Math.tan(a * Math.PI / 180)
              * 4, result.getDouble(6, curI), tolerance);
          assertEquals(!(false || e && true), result.getBoolean(7, curI));
          assertEquals(a, result.getLong(8, curI));
        }
        resultSize += result.numTuples();
      }
    }
    assertEquals(NUM_TUPLES, resultSize);
    apply.close();
  }

  @Test
  public void testComparisonApply() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("a", "b"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < SMALL_NUM_TUPLES; i++) {
      tbb.putLong(0, i + 1);
      tbb.putLong(1, 2 * i);
    }
    ImmutableList.Builder<ObjectExpression> objectExpressions = ImmutableList.builder();

    ExpressionOperator vara = new VariableExpression(0);
    ExpressionOperator varb = new VariableExpression(1);

    {
      // Expression: vara == varb;

      ExpressionOperator eq = new EqualsExpression(vara, varb);
      ObjectExpression expr = new ObjectExpression("equals", eq);

      objectExpressions.add(expr);
    }

    {
      // Expression: vara != varb;

      ExpressionOperator neq = new NotEqualsExpression(vara, varb);
      ObjectExpression expr = new ObjectExpression("notequals", neq);

      objectExpressions.add(expr);
    }

    {
      // Expression: vara > varb;

      ExpressionOperator gt = new GreaterThanExpression(vara, varb);
      ObjectExpression expr = new ObjectExpression("greaterthan", gt);

      objectExpressions.add(expr);
    }

    {
      // Expression: vara < varb;

      ExpressionOperator lt = new LessThanExpression(vara, varb);
      ObjectExpression expr = new ObjectExpression("lessthan", lt);

      objectExpressions.add(expr);
    }

    {
      // Expression: vara >= varb;

      ExpressionOperator gteq = new GreaterThanOrEqualsExpression(vara, varb);
      ObjectExpression expr = new ObjectExpression("greaterthanequals", gteq);

      objectExpressions.add(expr);
    }

    {
      // Expression: vara <= varb;

      ExpressionOperator lteq = new LessThanOrEqualsExpression(vara, varb);
      ObjectExpression expr = new ObjectExpression("lessthanequals", lteq);

      objectExpressions.add(expr);
    }

    Apply apply = new Apply(new TupleSource(tbb), objectExpressions.build());

    apply.open(null);
    TupleBatch result;
    int resultSize = 0;
    while (!apply.eos()) {
      result = apply.nextReady();
      if (result != null) {
        assertEquals(6, result.getSchema().numColumns());
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(0));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(1));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(2));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(3));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(4));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(5));

        for (int curI = 0; curI < result.numTuples(); curI++) {
          long i = curI + resultSize;
          long a = i + 1;
          long b = 2 * i;
          assertEquals(a == b, result.getBoolean(0, curI));
          assertEquals(a != b, result.getBoolean(1, curI));
          assertEquals(a > b, result.getBoolean(2, curI));
          assertEquals(a < b, result.getBoolean(3, curI));
          assertEquals(a >= b, result.getBoolean(4, curI));
          assertEquals(a <= b, result.getBoolean(5, curI));
        }
        resultSize += result.numTuples();
      }
    }
    assertEquals(SMALL_NUM_TUPLES, resultSize);
    apply.close();
  }

  @Test
  public void testStringComparisonApply() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE), ImmutableList.of("c", "d"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < SMALL_NUM_TUPLES; i++) {
      tbb.putString(0, "Foo" + i);
      tbb.putString(1, "Foo" + (2 - i));
    }
    ImmutableList.Builder<ObjectExpression> objectExpressions = ImmutableList.builder();

    ExpressionOperator varc = new VariableExpression(0);
    ExpressionOperator vard = new VariableExpression(1);

    {
      // Expression: varc == vard;

      ExpressionOperator eq = new EqualsExpression(varc, vard);
      ObjectExpression expr = new ObjectExpression("s_equals", eq);

      objectExpressions.add(expr);
    }

    {
      // Expression: varc != vard;

      ExpressionOperator neq = new NotEqualsExpression(varc, vard);
      ObjectExpression expr = new ObjectExpression("s_notequals", neq);

      objectExpressions.add(expr);
    }

    {
      // Expression: varc > vard;

      ExpressionOperator gt = new GreaterThanExpression(varc, vard);
      ObjectExpression expr = new ObjectExpression("s_greaterthan", gt);

      objectExpressions.add(expr);
    }

    {
      // Expression: varc < vard;

      ExpressionOperator lt = new LessThanExpression(varc, vard);
      ObjectExpression expr = new ObjectExpression("s_lessthan", lt);

      objectExpressions.add(expr);
    }

    {
      // Expression: varc >= vard;

      ExpressionOperator gteq = new GreaterThanOrEqualsExpression(varc, vard);
      ObjectExpression expr = new ObjectExpression("s_greaterthanequals", gteq);

      objectExpressions.add(expr);
    }

    {
      // Expression: varc <= vard;

      ExpressionOperator lteq = new LessThanOrEqualsExpression(varc, vard);
      ObjectExpression expr = new ObjectExpression("s_lessthanequals", lteq);

      objectExpressions.add(expr);
    }

    Apply apply = new Apply(new TupleSource(tbb), objectExpressions.build());

    apply.open(null);
    TupleBatch result;
    int resultSize = 0;
    while (!apply.eos()) {
      result = apply.nextReady();
      if (result != null) {
        assertEquals(6, result.getSchema().numColumns());
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(0));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(1));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(2));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(3));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(4));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(5));

        for (int curI = 0; curI < result.numTuples(); curI++) {
          long i = curI + resultSize;
          String c = "Foo" + (2 - i);
          String d = "Foo" + i;

          assertEquals(c.compareTo(d) == 0, result.getBoolean(0, curI));
          assertEquals(c.compareTo(d) != 0, result.getBoolean(1, curI));
          assertEquals(c.compareTo(d) > 0, result.getBoolean(2, curI));
          assertEquals(c.compareTo(d) < 0, result.getBoolean(3, curI));
          assertEquals(c.compareTo(d) >= 0, result.getBoolean(4, curI));
          assertEquals(c.compareTo(d) <= 0, result.getBoolean(5, curI));
        }
        resultSize += result.numTuples();
      }
    }
    assertEquals(SMALL_NUM_TUPLES, resultSize);
    apply.close();
  }
}
