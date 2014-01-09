package edu.washington.escience.myria.operator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.GenericEvaluator;
import edu.washington.escience.myria.expression.TupleEvaluator;

/**
 * Generic apply operator.
 */
public class Apply extends UnaryOperator {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * List of expressions that will be used to create the output.
   */
  private ImmutableList<Expression> expressions;

  /**
   * Evaluators for generic stateless expressions. One evaluator per expression in {@link #expressions}.
   */
  private ArrayList<GenericEvaluator> evaluators;

  /**
   * Buffers the output tuples.
   */
  private TupleBatchBuffer resultBuffer;

  /**
   * @return the resultBuffer
   */
  public TupleBatchBuffer getResultBuffer() {
    return resultBuffer;
  }

  /**
   * @return the expressions
   */
  protected ImmutableList<Expression> getExpressions() {
    return expressions;
  }

  /**
   * @return the evaluators
   */
  public ArrayList<GenericEvaluator> getEvaluators() {
    return evaluators;
  }

  /**
   * 
   * @param child child operator that data is fetched from
   * @param expressions expression that created the output
   */
  public Apply(final Operator child, final List<Expression> expressions) {
    super(child);
    if (expressions != null) {
      setExpressions(expressions);
    }
  }

  /**
   * Set the expressions for each column.
   * 
   * @param expressions the expressions
   */
  private void setExpressions(final List<Expression> expressions) {
    this.expressions = ImmutableList.copyOf(expressions);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    if (getChild().eoi() || getChild().eos()) {
      return resultBuffer.popAny();
    }

    while ((tb = getChild().nextReady()) != null) {
      for (int rowIdx = 0; rowIdx < tb.numTuples(); rowIdx++) {
        int columnIdx = 0;
        for (GenericEvaluator evaluator : evaluators) {
          try {
            evaluator.evalAndPut(tb, rowIdx, resultBuffer, columnIdx);
          } catch (InvocationTargetException e) {
            throw new DbException(e);
          }
          columnIdx++;
        }
      }
      if (resultBuffer.hasFilledTB()) {
        return resultBuffer.popFilled();
      }
    }
    if (getChild().eoi() || getChild().eos()) {
      return resultBuffer.popAny();
    } else {
      return resultBuffer.popFilled();
    }
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkNotNull(expressions);

    Schema inputSchema = getChild().getSchema();

    evaluators = new ArrayList<>();
    evaluators.ensureCapacity(expressions.size());
    for (Expression expr : expressions) {
      GenericEvaluator evaluator = new GenericEvaluator(expr, inputSchema);
      if (evaluator.needsCompiling()) {
        evaluator.compile();
      }
      evaluators.add(evaluator);
    }

    resultBuffer = new TupleBatchBuffer(getSchema());
  }

  @Override
  public Schema generateSchema() {
    if (expressions == null) {
      return null;
    }
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    Schema childSchema = child.getSchema();
    if (childSchema == null) {
      return null;
    }

    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();

    for (TupleEvaluator evaluator : evaluators) {
      evaluator.setSchema(childSchema);
      typesBuilder.add(evaluator.getOutputType());
      namesBuilder.add(evaluator.getOutputName());
    }
    return new Schema(typesBuilder.build(), namesBuilder.build());
  }
}
