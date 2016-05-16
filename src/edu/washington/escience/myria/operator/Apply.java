package edu.washington.escience.myria.operator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ConstantEvaluator;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Generic apply operator.
 */
public class Apply extends UnaryOperator {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * List of expressions that will be used to create the output.
   */
  private ImmutableList<Expression> emitExpressions;

  /**
   * One evaluator for each expression in {@link #emitExpressions}.
   */
  private ArrayList<GenericEvaluator> emitEvaluators;

  /**
   * @return the {@link #emitExpressions}
   */
  protected ImmutableList<Expression> getEmitExpressions() {
    return emitExpressions;
  }

  /**
   * @return the {@link #emitEvaluators}
   */
  public ArrayList<GenericEvaluator> getEmitEvaluators() {
    return emitEvaluators;
  }

  /**
   *
   * @param child child operator that data is fetched from
   * @param emitExpressions expression that created the output
   */
  public Apply(final Operator child, final List<Expression> emitExpressions) {
    super(child);
    if (emitExpressions != null) {
      setEmitExpressions(emitExpressions);
    }
  }

  /**
   * @param emitExpressions the emit expressions for each column
   */
  private void setEmitExpressions(final List<Expression> emitExpressions) {
    this.emitExpressions = ImmutableList.copyOf(emitExpressions);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException, InvocationTargetException {
    Operator child = getChild();

    if (child.eoi() || child.eos()) {
      return null;
    }

    TupleBatch tb = child.nextReady();
    if (tb == null) {
      return null;
    }

    List<Column<?>> output = Lists.newLinkedList();
    for (GenericEvaluator evaluator : emitEvaluators) {
      output.add(evaluator.evaluateColumn(tb));
    }
    return new TupleBatch(getSchema(), output);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkNotNull(emitExpressions);

    Schema inputSchema = Objects.requireNonNull(getChild().getSchema());

    emitEvaluators = new ArrayList<>(emitExpressions.size());
    final ExpressionOperatorParameter parameters =
        new ExpressionOperatorParameter(inputSchema, getNodeID());
    for (Expression expr : emitExpressions) {
      GenericEvaluator evaluator;
      if (expr.isConstant()) {
        evaluator = new ConstantEvaluator(expr, parameters);
      } else {
        evaluator = new GenericEvaluator(expr, parameters);
      }
      if (evaluator.needsCompiling()) {
        evaluator.compile();
      }
      Preconditions.checkArgument(!evaluator.needsState());
      emitEvaluators.add(evaluator);
    }
  }

  /**
   * @param evaluators the evaluators to set
   */
  public void setEvaluators(final ArrayList<GenericEvaluator> evaluators) {
    emitEvaluators = evaluators;
  }

  @Override
  public Schema generateSchema() {
    if (emitExpressions == null) {
      return null;
    }
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    Schema inputSchema = child.getSchema();
    if (inputSchema == null) {
      return null;
    }

    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();

    for (Expression expr : emitExpressions) {
      typesBuilder.add(expr.getOutputType(new ExpressionOperatorParameter(inputSchema)));
      namesBuilder.add(expr.getOutputName());
    }
    return new Schema(typesBuilder.build(), namesBuilder.build());
  }
}
