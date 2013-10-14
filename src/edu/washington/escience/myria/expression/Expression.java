package edu.washington.escience.myria.expression;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.ExpressionEncoding;

/**
 * An expression that can be applied to a tuple.
 */
public class Expression implements Serializable {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Name of the column that the result will be written to.
   */
  private final String outputName;

  /**
   * The java expression to be evaluated in {@link #eval}.
   */
  private String javaExpression;

  /**
   * Expression encoding reference is needed to get the output type.
   */
  private final ExpressionEncoding expressionEncoding;

  /**
   * 
   */
  private Evaluator evaluator;

  /**
   * The output type of the expression.
   */
  @SuppressWarnings("unused")
  private Type outputType;

  /**
   * Constructs the Expression object.
   * 
   * @param outputName the name of the resulting element
   * @param expressionEncoding Expression encoding that created this expression. Necessary to get output type.
   */
  public Expression(final String outputName, final ExpressionEncoding expressionEncoding) {
    this.outputName = outputName;
    this.expressionEncoding = expressionEncoding;
  }

  /**
   * Compiles the {@link #javaExpression}.
   * 
   * @param inputSchema the input schema
   * @param outputType the output type
   * @throws DbException compilation failed
   */
  @SuppressWarnings("deprecation")
  public void compile(final Schema inputSchema, final Type outputType) throws DbException {
    javaExpression = expressionEncoding.getJavaString(inputSchema);
    this.outputType = outputType;

    try {
      evaluator =
          (Evaluator) ExpressionEvaluator.createFastExpressionEvaluator(javaExpression, Evaluator.class, new String[] {
              "tb", "rowId" }, (ClassLoader) null);
    } catch (CompileException e) {
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
    return evaluator.evaluate(tb, rowId);
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
