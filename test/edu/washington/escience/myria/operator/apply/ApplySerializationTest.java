package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.expression.AbsExpression;
import edu.washington.escience.myria.expression.AndExpression;
import edu.washington.escience.myria.expression.CastExpression;
import edu.washington.escience.myria.expression.CeilExpression;
import edu.washington.escience.myria.expression.ConditionalExpression;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.CosExpression;
import edu.washington.escience.myria.expression.DivideExpression;
import edu.washington.escience.myria.expression.EqualsExpression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.FloorExpression;
import edu.washington.escience.myria.expression.GreaterExpression;
import edu.washington.escience.myria.expression.GreaterThanExpression;
import edu.washington.escience.myria.expression.GreaterThanOrEqualsExpression;
import edu.washington.escience.myria.expression.HashMd5Expression;
import edu.washington.escience.myria.expression.IntDivideExpression;
import edu.washington.escience.myria.expression.LenExpression;
import edu.washington.escience.myria.expression.LessThanExpression;
import edu.washington.escience.myria.expression.LessThanOrEqualsExpression;
import edu.washington.escience.myria.expression.LesserExpression;
import edu.washington.escience.myria.expression.LogExpression;
import edu.washington.escience.myria.expression.MinusExpression;
import edu.washington.escience.myria.expression.ModuloExpression;
import edu.washington.escience.myria.expression.NegateExpression;
import edu.washington.escience.myria.expression.NotEqualsExpression;
import edu.washington.escience.myria.expression.NotExpression;
import edu.washington.escience.myria.expression.OrExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.PowExpression;
import edu.washington.escience.myria.expression.RandomExpression;
import edu.washington.escience.myria.expression.SinExpression;
import edu.washington.escience.myria.expression.SqrtExpression;
import edu.washington.escience.myria.expression.StateExpression;
import edu.washington.escience.myria.expression.SubstrExpression;
import edu.washington.escience.myria.expression.TanExpression;
import edu.washington.escience.myria.expression.TimesExpression;
import edu.washington.escience.myria.expression.ToUpperCaseExpression;
import edu.washington.escience.myria.expression.TypeExpression;
import edu.washington.escience.myria.expression.TypeOfExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.expression.WorkerIdExpression;

public class ApplySerializationTest {

  @Test
  public void testJsonMapping() throws IOException {
    ObjectReader reader = MyriaJsonMapperProvider.getReader().withType(ExpressionOperator.class);
    ObjectWriter writer = MyriaJsonMapperProvider.getWriter();

    ImmutableList.Builder<ExpressionOperator> expressions = ImmutableList.builder();

    /* Zeroary */
    ConstantExpression constant = new ConstantExpression(Type.INT_TYPE, "5");
    RandomExpression random = new RandomExpression();
    StateExpression state = new StateExpression(3);
    TypeExpression type = new TypeExpression(Type.INT_TYPE);
    TypeOfExpression typeof = new TypeOfExpression(2);
    VariableExpression variable = new VariableExpression(1);
    WorkerIdExpression worker = new WorkerIdExpression();
    expressions
        .add(constant)
        .add(random)
        .add(state)
        .add(type)
        .add(typeof)
        .add(variable)
        .add(worker);

    /* Unary */
    AbsExpression abs = new AbsExpression(constant);
    CeilExpression ceil = new CeilExpression(constant);
    FloorExpression floor = new FloorExpression(constant);
    LogExpression log = new LogExpression(constant);
    HashMd5Expression md5 = new HashMd5Expression(constant);
    NegateExpression negate = new NegateExpression(constant);
    CosExpression cos = new CosExpression(constant);
    SinExpression sin = new SinExpression(constant);
    SqrtExpression sqrt = new SqrtExpression(constant);
    TanExpression tan = new TanExpression(constant);
    ToUpperCaseExpression upper = new ToUpperCaseExpression(constant);
    NotExpression not = new NotExpression(constant);
    LenExpression len = new LenExpression(constant);
    expressions
        .add(abs)
        .add(ceil)
        .add(cos)
        .add(floor)
        .add(log)
        .add(md5)
        .add(negate)
        .add(not)
        .add(sin)
        .add(sqrt)
        .add(tan)
        .add(upper)
        .add(len);

    /* Binary */
    DivideExpression divide = new DivideExpression(constant, variable);
    IntDivideExpression idivide = new IntDivideExpression(constant, variable);
    MinusExpression minus = new MinusExpression(constant, variable);
    PlusExpression plus = new PlusExpression(constant, variable);
    PowExpression pow = new PowExpression(constant, variable);
    TimesExpression times = new TimesExpression(constant, variable);
    AndExpression and = new AndExpression(constant, variable);
    OrExpression or = new OrExpression(constant, variable);
    EqualsExpression eq = new EqualsExpression(constant, variable);
    NotEqualsExpression ne = new NotEqualsExpression(constant, variable);
    GreaterThanExpression gt = new GreaterThanExpression(constant, variable);
    LessThanExpression lt = new LessThanExpression(constant, variable);
    GreaterThanOrEqualsExpression gte = new GreaterThanOrEqualsExpression(constant, variable);
    LessThanOrEqualsExpression lte = new LessThanOrEqualsExpression(constant, variable);
    CastExpression cast = new CastExpression(constant, typeof);
    ModuloExpression modulo = new ModuloExpression(constant, variable);
    LesserExpression min = new LesserExpression(constant, variable);
    GreaterExpression max = new GreaterExpression(constant, variable);
    expressions
        .add(and)
        .add(divide)
        .add(idivide)
        .add(eq)
        .add(gt)
        .add(gte)
        .add(lt)
        .add(lte)
        .add(minus)
        .add(ne)
        .add(or)
        .add(plus)
        .add(pow)
        .add(times)
        .add(cast)
        .add(modulo)
        .add(min)
        .add(max);

    /* NAry */
    VariableExpression variable2 = new VariableExpression(2);
    ConditionalExpression conditional = new ConditionalExpression(variable, constant, variable2);
    SubstrExpression substr = new SubstrExpression(constant, constant, constant);
    expressions.add(conditional).add(substr);

    /* Test serializing and deserializing all of them. */
    for (ExpressionOperator op : expressions.build()) {
      assertTrue(writer.canSerialize(op.getClass()));
      String serialized = writer.writeValueAsString(op);
      ExpressionOperator op2 = reader.readValue(serialized);
      assertEquals(op2, op);
    }
  }
}
