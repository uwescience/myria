package edu.washington.escience.myria.expression;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.codehaus.janino.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Parser.ParseException;
import org.codehaus.janino.Scanner.ScanException;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.ExpressionEncoding;

/**
 * An expression that can be applied to a tuple.
 */
public class Expression {
  /**
   * Name of the column that the result will be written to.
   */
  private final String outputName;

  /**
   * The java expression to be evaluated in {@link #eval}.
   */
  private final String javaExpression;

  /**
   * List of variables used in {@link #javaExpression}.
   */
  private final List<VariableExpression> indexes;

  /**
   * Expression encoding reference is needed to get the output type.
   */
  private final ExpressionEncoding expressionEncoding;

  /**
   * The janino expression evaluator.
   */
  private ExpressionEvaluator evaluator;

  /**
   * Array of arguments passed to {@link #evaluator}.
   */
  private final ThreadLocal<Object[]> args;

  /**
   * Constructs the Expression object.
   * 
   * @param outputName the name of the resulting element
   * @param javaExpression the expression to be evaluated
   * @param indexes variables that are used in the javaExpression
   * @param expressionEncoding Expression encoding that created this expression. Necessary to get output type.
   */
  public Expression(final String outputName, final String javaExpression, final List<VariableExpression> indexes,
      final ExpressionEncoding expressionEncoding) {
    this.outputName = outputName;
    this.javaExpression = javaExpression;
    this.indexes = indexes;
    this.expressionEncoding = expressionEncoding;

    args = new ThreadLocal<Object[]>() {
      @Override
      protected Object[] initialValue() {
        return new Object[indexes.size()];
      }
    };
  }

  /**
   * Compiles the {@link #javaExpression}.
   * 
   * @param inputSchema the input schema
   * @param outputType the output type
   * @throws DbException compilation failed
   */
  public void compile(final Schema inputSchema, final Type outputType) throws DbException {
    String[] parameterNames = new String[indexes.size()];
    Class<?>[] parameterTypes = new Class[indexes.size()];

    int i = 0;
    for (VariableExpression var : indexes) {
      parameterNames[i] = var.getJavaString();
      parameterTypes[i] = inputSchema.getColumnType(i).toJavaType();
      i++;
    }

    Class<?> javaOutputType = outputType.toJavaType();

    try {
      evaluator = new ExpressionEvaluator(javaExpression, javaOutputType, parameterNames, parameterTypes);
    } catch (CompileException | ParseException | ScanException e) {
      throw new DbException("Error when compiling expression " + this, e);
    }
  }

  /**
   * Evaluates the expression.
   * 
   * @param tb a tuple batch
   * @param rowId the row that should be used for input data
   * @return the result from the evaluation
   * @throws InvocationTargetException exception thrown from janino
   */
  public Object eval(final TupleBatch tb, final int rowId) throws InvocationTargetException {
    Preconditions.checkArgument(evaluator != null, "Call compile first.");

    int i = 0;
    for (VariableExpression var : indexes) {
      args.get()[i++] = tb.getObject(var.getColumnIdx(), rowId);
    }
    // System.out.println(this + " on " + args[0] + " is " + evaluator.evaluate(args));
    return evaluator.evaluate(args.get());
  }

  /**
   * @return the output name
   */
  public String getOutputName() {
    return outputName;
  }

  /**
   * @param schema the input schema
   * @return the type of the output
   */
  public Type getOutputType(final Schema schema) {
    return expressionEncoding.getOutputType(schema);
  }

  @Override
  public String toString() {
    return "Expression: " + javaExpression;
  }
}
