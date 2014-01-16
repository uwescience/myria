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
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.expression.evaluate.TupleEvaluator;

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
   * @return the {@link #emitExpressions}
   */
  protected ImmutableList<Expression> getEmitExpressions() {
    return emitExpressions;
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
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    if (getChild().eoi() || getChild().eos()) {
      return resultBuffer.popAny();
    }

    while ((tb = getChild().nextReady()) != null) {
      fillBuffer(tb);
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

  /**
   * @param tb the source tuple batch
   * @throws DbException thrown if something goes wrong during evaluation
   */
  protected void fillBuffer(final TupleBatch tb) throws DbException {
    for (int rowIdx = 0; rowIdx < tb.numTuples(); rowIdx++) {
      for (int columnIdx = 0; columnIdx < getSchema().numColumns(); columnIdx++) {
        try {
          getEvaluator(columnIdx).evalAndPut(tb, rowIdx, resultBuffer, columnIdx, null);
        } catch (InvocationTargetException e) {
          throw new DbException(e);
        }
      }
    }
  }

  /**
   * Called in {@link #fetchNextReady()}.
   * 
   * @param tb the source tuple batch
   * @param rowIdx the current row index
   * @param columnIdx the current column index
   * @throws InvocationTargetException exception when evaluating
   */
  protected void evaluate(final TupleBatch tb, final int rowIdx, final int columnIdx) throws InvocationTargetException {

  }

  /**
   * @param columnIdx the column index
   * @return evaluator for column index
   */
  protected GenericEvaluator getEvaluator(final int columnIdx) {
    return evaluators.get(columnIdx);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkNotNull(emitExpressions);

    Schema inputSchema = getChild().getSchema();

    evaluators = new ArrayList<>();
    evaluators.ensureCapacity(emitExpressions.size());
    for (Expression expr : emitExpressions) {
      GenericEvaluator evaluator = new GenericEvaluator(expr, inputSchema, null);
      if (evaluator.needsCompiling()) {
        evaluator.compile();
      }
      evaluators.add(evaluator);
    }

    resultBuffer = new TupleBatchBuffer(getSchema());
  }

  /**
   * @param evaluators the evaluators to set
   */
  public void setEvaluators(final ArrayList<GenericEvaluator> evaluators) {
    this.evaluators = evaluators;
  }

  /**
   * @param resultBuffer the resultBuffer to set
   */
  public void setResultBuffer(final TupleBatchBuffer resultBuffer) {
    this.resultBuffer = resultBuffer;
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
    Schema childSchema = child.getSchema();
    if (childSchema == null) {
      return null;
    }

    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();

    for (TupleEvaluator evaluator : evaluators) {
      evaluator.setInputSchema(childSchema);
      typesBuilder.add(evaluator.getOutputType());
      namesBuilder.add(evaluator.getOutputName());
    }
    return new Schema(typesBuilder.build(), namesBuilder.build());
  }
}
