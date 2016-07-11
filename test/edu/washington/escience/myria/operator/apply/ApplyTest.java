package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.AbsExpression;
import edu.washington.escience.myria.expression.AndExpression;
import edu.washington.escience.myria.expression.CeilExpression;
import edu.washington.escience.myria.expression.ConditionalExpression;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.CosExpression;
import edu.washington.escience.myria.expression.DivideExpression;
import edu.washington.escience.myria.expression.EqualsExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.FloorExpression;
import edu.washington.escience.myria.expression.GreaterExpression;
import edu.washington.escience.myria.expression.GreaterThanExpression;
import edu.washington.escience.myria.expression.GreaterThanOrEqualsExpression;
import edu.washington.escience.myria.expression.HashMd5Expression;
import edu.washington.escience.myria.expression.LenExpression;
import edu.washington.escience.myria.expression.LessThanExpression;
import edu.washington.escience.myria.expression.LessThanOrEqualsExpression;
import edu.washington.escience.myria.expression.LesserExpression;
import edu.washington.escience.myria.expression.MinusExpression;
import edu.washington.escience.myria.expression.ModuloExpression;
import edu.washington.escience.myria.expression.NotEqualsExpression;
import edu.washington.escience.myria.expression.NotExpression;
import edu.washington.escience.myria.expression.OrExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.PowExpression;
import edu.washington.escience.myria.expression.RandomExpression;
import edu.washington.escience.myria.expression.SinExpression;
import edu.washington.escience.myria.expression.SqrtExpression;
import edu.washington.escience.myria.expression.SubstrExpression;
import edu.washington.escience.myria.expression.TanExpression;
import edu.washington.escience.myria.expression.TimesExpression;
import edu.washington.escience.myria.expression.ToUpperCaseExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.expression.WorkerIdExpression;
import edu.washington.escience.myria.expression.evaluate.ConstantEvaluator;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class ApplyTest {

  private final int NUM_TUPLES = 2 * TupleBatch.BATCH_SIZE;
  private final int SMALL_NUM_TUPLES = 10;

  @Test
  public void testApply() throws DbException {
    final Schema schema =
        new Schema(
            ImmutableList.of(
                Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE, Type.STRING_TYPE, Type.BOOLEAN_TYPE),
            ImmutableList.of("a", "b", "c", "d", "e"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < NUM_TUPLES; i++) {
      tbb.putLong(0, (long) Math.pow(i, 2));
      tbb.putLong(1, i + 1);
      tbb.putInt(2, (int) i);
      tbb.putString(3, "Foo" + i);
      tbb.putBoolean(4, i % 2 == 0);
    }
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();

    ExpressionOperator vara = new VariableExpression(0);
    ExpressionOperator varb = new VariableExpression(1);
    ExpressionOperator varc = new VariableExpression(2);
    ExpressionOperator vard = new VariableExpression(3);
    ExpressionOperator vare = new VariableExpression(4);

    {
      // Expression: Math.sqrt(a);

      ExpressionOperator squareRoot = new SqrtExpression(vara);

      Expression expr = new Expression("sqrt", squareRoot);

      Expressions.add(expr);
    }

    {
      // Expression: (b+c) * (b-c)

      ExpressionOperator plus = new PlusExpression(varb, varc);
      ExpressionOperator minus = new MinusExpression(varb, varc);

      ExpressionOperator times = new TimesExpression(plus, minus);

      Expression expr = new Expression("simpleNestedExpression", times);
      Expressions.add(expr);
    }

    {
      // Expression: Math.sqrt(Math.pow(a, 2) + Math.pow(b, 2))

      ExpressionOperator two = new ConstantExpression(2);
      ExpressionOperator pow1 = new PowExpression(vara, two);
      ExpressionOperator pow2 = new PowExpression(varb, two);

      ExpressionOperator plus = new PlusExpression(pow1, pow2);

      ExpressionOperator sqrt = new SqrtExpression(plus);

      Expression expr = new Expression("distance", sqrt);
      Expressions.add(expr);
    }

    {
      // Expression: d.toUpperCase()

      ExpressionOperator upper = new ToUpperCaseExpression(vard);

      Expression expr = new Expression("upper", upper);
      Expressions.add(expr);
    }

    {
      // Expression: Math.abs(b-a)

      ExpressionOperator abs = new AbsExpression(new MinusExpression(varb, vara));

      Expression expr = new Expression("absolute", abs);
      Expressions.add(expr);
    }

    {
      // Expression: Math.floor(Math.sqrt(a)) + Math.ceil(Math.sqrt(a));

      ExpressionOperator squareRoot = new SqrtExpression(vara);
      ExpressionOperator floor = new FloorExpression(squareRoot);
      ExpressionOperator ceil = new CeilExpression(squareRoot);
      ExpressionOperator plus = new PlusExpression(floor, ceil);

      Expression expr = new Expression("floorCeil", plus);

      Expressions.add(expr);
    }

    {
      // Expression: Math.cos(a * Math.PI / 180) * 2 + Math.sin(a * Math.PI / 180) * 3 + Math.tan(a * Math.PI / 180) *
      // 4;

      ExpressionOperator angle =
          new DivideExpression(
              new TimesExpression(vara, new ConstantExpression(Type.DOUBLE_TYPE, "Math.PI")),
              new ConstantExpression(180));
      ExpressionOperator cos =
          new TimesExpression(new CosExpression(angle), new ConstantExpression(2));
      ExpressionOperator sin =
          new TimesExpression(new SinExpression(angle), new ConstantExpression(3));
      ExpressionOperator tan =
          new TimesExpression(new TanExpression(angle), new ConstantExpression(4));
      ExpressionOperator add = new PlusExpression(new PlusExpression(cos, sin), tan);

      Expression expr = new Expression("trig", add);

      Expressions.add(expr);
    }

    {
      // Expression: !(false || e && true);

      ExpressionOperator and = new AndExpression(vare, new ConstantExpression(true));
      ExpressionOperator or = new OrExpression(new ConstantExpression(false), and);
      ExpressionOperator not = new NotExpression(or);
      Expression expr = new Expression("boolean", not);

      Expressions.add(expr);
    }

    final ExpressionOperatorParameter parameters =
        new ExpressionOperatorParameter(tbb.getSchema(), -1);
    {
      // Expression (just copy/ rename): a;
      Expression expr = new Expression("copy", vara);

      GenericEvaluator eval = new GenericEvaluator(expr, parameters);
      assertTrue(!eval.needsCompiling());
      Expressions.add(expr);
    }

    {
      // Expression: (a constant value of 5);
      Expression expr = new Expression("constant5", new ConstantExpression(5));

      GenericEvaluator eval = new ConstantEvaluator(expr, parameters);
      assertTrue(!eval.needsCompiling());
      Expressions.add(expr);
    }

    {
      // Expression: (a constant value of 5.0 float);
      Expression expr = new Expression("constant5f", new ConstantExpression(5.0f));

      GenericEvaluator eval = new ConstantEvaluator(expr, parameters);
      assertTrue(!eval.needsCompiling());
      Expressions.add(expr);
    }

    {
      // Expression: (a constant value of 5.0 double);
      Expression expr = new Expression("constant5d", new ConstantExpression(5d));

      GenericEvaluator eval = new ConstantEvaluator(expr, parameters);
      assertTrue(!eval.needsCompiling());
      Expressions.add(expr);
    }

    {
      // Expression: (a random double);
      Expression expr = new Expression("random", new RandomExpression());

      GenericEvaluator eval = new GenericEvaluator(expr, parameters);
      assertTrue(eval.needsCompiling());
      Expressions.add(expr);
    }

    {
      // Expression: c % b;
      Expression expr =
          new Expression(
              "modulo", new ModuloExpression(new VariableExpression(2), new VariableExpression(1)));

      GenericEvaluator eval = new GenericEvaluator(expr, parameters);
      assertTrue(eval.needsCompiling());
      Expressions.add(expr);
    }

    {
      // Expression: e ? a : c;
      Expression expr =
          new Expression(
              "conditional",
              new ConditionalExpression(
                  new VariableExpression(4), new VariableExpression(0), new VariableExpression(2)));

      GenericEvaluator eval = new GenericEvaluator(expr, parameters);
      assertTrue(eval.needsCompiling());
      Expressions.add(expr);
    }

    {
      // Nested case
      // Expression: (b % 2 == 0) ? (e: a : b) : c;
      Expression expr =
          new Expression(
              "nestedconditional",
              new ConditionalExpression(
                  new EqualsExpression(
                      new ModuloExpression(new VariableExpression(1), new ConstantExpression(2)),
                      new ConstantExpression(0)),
                  new ConditionalExpression(
                      new VariableExpression(4),
                      new VariableExpression(0),
                      new VariableExpression(1)),
                  new VariableExpression(2)));

      GenericEvaluator eval = new GenericEvaluator(expr, parameters);
      assertTrue(eval.needsCompiling());
      Expressions.add(expr);
    }

    {
      // Expression that returns the worker id (set below as nodeId)
      Expression expr = new Expression("workerID", new WorkerIdExpression());

      GenericEvaluator eval =
          new ConstantEvaluator(expr, new ExpressionOperatorParameter(tbb.getSchema(), 42));
      assertTrue(!eval.needsCompiling());
      assertEquals(eval.getJavaExpressionWithAppend(), "result.appendInt(42)");
      Expressions.add(expr);
    }

    {
      // Expression: d.length()
      ExpressionOperator len = new LenExpression(vard);
      Expression expr = new Expression("len", len);
      Expressions.add(expr);
    }

    {
      // Expression: max(a,c)
      GreaterExpression max = new GreaterExpression(vara, varc);
      Expression expr = new Expression("max", max);
      Expressions.add(expr);
    }

    {
      // Expression: min(a,c)
      LesserExpression min = new LesserExpression(vara, varc);
      Expression expr = new Expression("min", min);
      Expressions.add(expr);
    }

    {
      // Expression: d.substring(0,4);
      SubstrExpression substr =
          new SubstrExpression(vard, new ConstantExpression(0), new ConstantExpression(4));
      Expression expr = new Expression("substr", substr);
      Expressions.add(expr);
    }

    {
      // Expression: hash(a);
      HashMd5Expression hash = new HashMd5Expression(vara);
      Expression expr = new Expression("hashA", hash);
      Expressions.add(expr);
    }

    {
      // Expression: hash(c);
      HashMd5Expression hash = new HashMd5Expression(varc);
      Expression expr = new Expression("hashC", hash);
      Expressions.add(expr);
    }

    {
      // Expression: hash(d);
      HashMd5Expression hash = new HashMd5Expression(vard);
      Expression expr = new Expression("hashD", hash);
      Expressions.add(expr);
    }

    Apply apply = new Apply(new TupleSource(tbb), Expressions.build());

    final int nodeId = 3;
    apply.open(TestEnvVars.get(nodeId));
    TupleBatch result;
    int resultSize = 0;
    final double tolerance = 0.0000001;
    while (!apply.eos()) {
      result = apply.nextReady();
      if (result != null) {
        assertEquals(24, result.getSchema().numColumns());
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(0));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(1));
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(2));
        assertEquals(Type.STRING_TYPE, result.getSchema().getColumnType(3));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(4));
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(5));
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(6));
        assertEquals(Type.BOOLEAN_TYPE, result.getSchema().getColumnType(7));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(8));
        assertEquals(Type.INT_TYPE, result.getSchema().getColumnType(9));
        assertEquals(Type.FLOAT_TYPE, result.getSchema().getColumnType(10));
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(11));
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(12));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(13));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(14));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(15));
        assertEquals(Type.INT_TYPE, result.getSchema().getColumnType(16));
        assertEquals(Type.INT_TYPE, result.getSchema().getColumnType(17));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(18));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(19));
        assertEquals(Type.STRING_TYPE, result.getSchema().getColumnType(20));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(21));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(22));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(23));

        assertEquals("sqrt", result.getSchema().getColumnName(0));
        assertEquals("simpleNestedExpression", result.getSchema().getColumnName(1));
        assertEquals("distance", result.getSchema().getColumnName(2));
        assertEquals("upper", result.getSchema().getColumnName(3));
        assertEquals("absolute", result.getSchema().getColumnName(4));
        assertEquals("floorCeil", result.getSchema().getColumnName(5));
        assertEquals("trig", result.getSchema().getColumnName(6));
        assertEquals("boolean", result.getSchema().getColumnName(7));
        assertEquals("copy", result.getSchema().getColumnName(8));
        assertEquals("constant5", result.getSchema().getColumnName(9));
        assertEquals("constant5f", result.getSchema().getColumnName(10));
        assertEquals("constant5d", result.getSchema().getColumnName(11));
        assertEquals("random", result.getSchema().getColumnName(12));
        assertEquals("modulo", result.getSchema().getColumnName(13));
        assertEquals("conditional", result.getSchema().getColumnName(14));
        assertEquals("nestedconditional", result.getSchema().getColumnName(15));
        assertEquals("workerID", result.getSchema().getColumnName(16));
        assertEquals("len", result.getSchema().getColumnName(17));
        assertEquals("max", result.getSchema().getColumnName(18));
        assertEquals("min", result.getSchema().getColumnName(19));
        assertEquals("substr", result.getSchema().getColumnName(20));
        assertEquals("hashA", result.getSchema().getColumnName(21));
        assertEquals("hashC", result.getSchema().getColumnName(22));
        assertEquals("hashD", result.getSchema().getColumnName(23));

        for (int curI = 0; curI < result.numTuples(); curI++) {
          long i = curI + resultSize;
          long a = (long) Math.pow(i, 2);
          long b = i + 1;
          int c = (int) i;
          String d = "Foo" + i;
          boolean e = i % 2 == 0;
          assertEquals(i, result.getDouble(0, curI), tolerance);
          assertEquals((b + c) * (b - c), result.getLong(1, curI));
          assertEquals(
              Math.sqrt(Math.pow(a, 2) + Math.pow(b, 2)), result.getDouble(2, curI), tolerance);
          assertEquals(d.toUpperCase(), result.getString(3, curI));
          assertEquals(Math.abs(b - a), result.getLong(4, curI));
          assertEquals(
              Math.floor(Math.sqrt(a)) + Math.ceil(Math.sqrt(a)),
              result.getDouble(5, curI),
              tolerance);
          assertEquals(
              Math.cos(a * Math.PI / 180) * 2
                  + Math.sin(a * Math.PI / 180) * 3
                  + Math.tan(a * Math.PI / 180) * 4,
              result.getDouble(6, curI),
              tolerance);
          assertEquals(!(false || e && true), result.getBoolean(7, curI));
          assertEquals(a, result.getLong(8, curI));
          assertEquals(5, result.getInt(9, curI));
          assertEquals((float) 5.0, result.getFloat(10, curI), 0.00001);
          assertEquals(5.0, result.getDouble(11, curI), 0.00001);
          assertTrue(0.0 <= result.getDouble(12, curI) && result.getDouble(12, curI) < 1.0);
          assertEquals(c % b, result.getLong(13, curI));
          assertEquals(e ? a : c, result.getLong(14, curI));
          assertEquals((b % 2 == 0) ? (e ? a : b) : c, result.getLong(15, curI));
          assertEquals(nodeId, result.getInt(16, curI));
          assertEquals(d.length(), result.getInt(17, curI));
          assertEquals(Math.max(a, c), result.getLong(18, curI));
          assertEquals(Math.min(a, c), result.getLong(19, curI));
          assertEquals(d.substring(0, 4), result.getString(20, curI));
          assertEquals(Hashing.md5().hashLong(a).asLong(), result.getLong(21, curI));
          assertEquals(Hashing.md5().hashInt(c).asLong(), result.getLong(22, curI));
          assertEquals(
              Hashing.md5().hashString(d, Charset.defaultCharset()).asLong(),
              result.getLong(23, curI));
        }
        resultSize += result.numTuples();
      }
    }
    assertEquals(NUM_TUPLES, resultSize);
    apply.close();
  }

  @Test
  public void testComparisonApply() throws DbException {
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("a", "b"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < SMALL_NUM_TUPLES; i++) {
      tbb.putLong(0, i + 1);
      tbb.putLong(1, 2 * i);
    }
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();

    ExpressionOperator vara = new VariableExpression(0);
    ExpressionOperator varb = new VariableExpression(1);

    {
      // Expression: vara == varb;

      ExpressionOperator eq = new EqualsExpression(vara, varb);
      Expression expr = new Expression("equals", eq);

      Expressions.add(expr);
    }

    {
      // Expression: vara != varb;

      ExpressionOperator neq = new NotEqualsExpression(vara, varb);
      Expression expr = new Expression("notequals", neq);

      Expressions.add(expr);
    }

    {
      // Expression: vara > varb;

      ExpressionOperator gt = new GreaterThanExpression(vara, varb);
      Expression expr = new Expression("greaterthan", gt);

      Expressions.add(expr);
    }

    {
      // Expression: vara < varb;

      ExpressionOperator lt = new LessThanExpression(vara, varb);
      Expression expr = new Expression("lessthan", lt);

      Expressions.add(expr);
    }

    {
      // Expression: vara >= varb;

      ExpressionOperator gteq = new GreaterThanOrEqualsExpression(vara, varb);
      Expression expr = new Expression("greaterthanequals", gteq);

      Expressions.add(expr);
    }

    {
      // Expression: vara <= varb;

      ExpressionOperator lteq = new LessThanOrEqualsExpression(vara, varb);
      Expression expr = new Expression("lessthanequals", lteq);

      Expressions.add(expr);
    }

    Apply apply = new Apply(new TupleSource(tbb), Expressions.build());

    apply.open(TestEnvVars.get());
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
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE), ImmutableList.of("c", "d"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < SMALL_NUM_TUPLES; i++) {
      tbb.putString(0, "Foo" + i);
      tbb.putString(1, "Foo" + (2 - i));
    }
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();

    ExpressionOperator varc = new VariableExpression(0);
    ExpressionOperator vard = new VariableExpression(1);

    {
      // Expression: varc == vard;

      ExpressionOperator eq = new EqualsExpression(varc, vard);
      Expression expr = new Expression("s_equals", eq);

      Expressions.add(expr);
    }

    {
      // Expression: varc != vard;

      ExpressionOperator neq = new NotEqualsExpression(varc, vard);
      Expression expr = new Expression("s_notequals", neq);

      Expressions.add(expr);
    }

    {
      // Expression: varc > vard;

      ExpressionOperator gt = new GreaterThanExpression(varc, vard);
      Expression expr = new Expression("s_greaterthan", gt);

      Expressions.add(expr);
    }

    {
      // Expression: varc < vard;

      ExpressionOperator lt = new LessThanExpression(varc, vard);
      Expression expr = new Expression("s_lessthan", lt);

      Expressions.add(expr);
    }

    {
      // Expression: varc >= vard;

      ExpressionOperator gteq = new GreaterThanOrEqualsExpression(varc, vard);
      Expression expr = new Expression("s_greaterthanequals", gteq);

      Expressions.add(expr);
    }

    {
      // Expression: varc <= vard;

      ExpressionOperator lteq = new LessThanOrEqualsExpression(varc, vard);
      Expression expr = new Expression("s_lessthanequals", lteq);

      Expressions.add(expr);
    }

    Apply apply = new Apply(new TupleSource(tbb), Expressions.build());

    apply.open(TestEnvVars.get());
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
          String c = "Foo" + i;
          String d = "Foo" + (2 - i);

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

  @Test(expected = IllegalArgumentException.class)
  public void conditionalNeedsBooleancondition() throws IllegalArgumentException {
    ExpressionOperator a = new ConstantExpression(Type.INT_TYPE, "1");
    ConditionalExpression conditional = new ConditionalExpression(a, a, a);
    conditional.getOutputType(new ExpressionOperatorParameter());
  }

  @Test(expected = IllegalArgumentException.class)
  public void conditionalMismatchedTypes() throws IllegalArgumentException {
    ConditionalExpression conditional =
        new ConditionalExpression(
            new ConstantExpression(Type.BOOLEAN_TYPE, "true"),
            new ConstantExpression(Type.INT_TYPE, "1"),
            new ConstantExpression(Type.STRING_TYPE, "foo"));
    conditional.getOutputType(new ExpressionOperatorParameter());
  }

  @Test(expected = IllegalArgumentException.class)
  public void moduloNeedsIntegers() throws IllegalArgumentException {
    ModuloExpression conditional =
        new ModuloExpression(
            new ConstantExpression(Type.FLOAT_TYPE, "1"),
            new ConstantExpression(Type.INT_TYPE, "2"));
    conditional.getOutputType(new ExpressionOperatorParameter());
  }
}
