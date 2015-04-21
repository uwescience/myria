package edu.washington.escience.myria.operator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.FlatteningGenericEvaluator;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.ListUtils;

/**
 * Generic apply operator for vector-valued expressions.
 */
public class FlatteningApply extends UnaryOperator {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * List (possibly empty) of expressions that will be used to create the output.
   */
  @Nonnull
  private ImmutableList<Expression> emitExpressions = ImmutableList.of();

  /**
   * One evaluator for each expression in {@link #emitExpressions}.
   */
  @Nonnull
  private ImmutableList<FlatteningGenericEvaluator<?>> emitEvaluators = ImmutableList.of();

  /**
   * Buffer to hold finished and in-progress TupleBatches.
   */
  private TupleBatchBuffer outputBuffer;

  /**
   * Indexes of columns from input relation that we should include in the result (with values duplicated for each result
   * in each expression evaluation). Must be an empty array if no columns are to be retained.
   */
  @Nonnull
  private int[] columnsToKeep = {};

  /**
   * @return the {@link #emitExpressions}
   */
  protected ImmutableList<Expression> getEmitExpressions() {
    return emitExpressions;
  }

  /**
   * @return the {@link #emitEvaluators}
   */
  public List<FlatteningGenericEvaluator<?>> getEmitEvaluators() {
    return emitEvaluators;
  }

  /**
   * 
   * @param child child operator that data is fetched from
   * @param emitExpressions expression that created the output
   * @param columnsToKeep indexes of columns to keep from input relation
   */
  public FlatteningApply(@Nonnull final Operator child, @Nonnull final List<Expression> emitExpressions,
      final int[] columnsToKeep) {
    super(child);
    Preconditions.checkNotNull(child);
    Preconditions.checkNotNull(emitExpressions);
    setEmitExpressions(emitExpressions);
    if (columnsToKeep != null) {
      this.columnsToKeep = columnsToKeep;
    }
  }

  /**
   * @param emitExpressions the emit expressions for each column
   */
  private void setEmitExpressions(@Nonnull final List<Expression> emitExpressions) {
    this.emitExpressions = ImmutableList.copyOf(emitExpressions);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException, InvocationTargetException {
    // If there's a batch already finished, return it, otherwise keep reading
    // batches from the child until we have a full batch or the child returns null.
    while (!outputBuffer.hasFilledTB()) {
      TupleBatch inputTuples = getChild().nextReady();
      if (inputTuples != null) {
        for (int rowIdx = 0; rowIdx < inputTuples.numTuples(); ++rowIdx) {
          // Call each evaluator on current row, then take Cartesian product of all output iterables from each
          // evaluator.
          List<Iterable<?>> evalResults = new ArrayList<>();
          for (FlatteningGenericEvaluator<?> evaluator : emitEvaluators) {
            Iterable<?> it = evaluator.eval(inputTuples, rowIdx, null);
            evalResults.add(it);
          }
          Iterable<Object[]> allCombinations = ListUtils.cartesianProduct(Object.class, evalResults);
          for (Object[] combination : allCombinations) {
            // Duplicate the values of all columns we are keeping from the original relation in this row.
            int colIdx = 0;
            for (int colKeepIdx : columnsToKeep) {
              TupleUtils.copyValue(inputTuples.asColumn(colKeepIdx), rowIdx, outputBuffer, colIdx++);
            }
            for (Object value : combination) {
              outputBuffer.put(colIdx++, value);
            }
          }
        }
      } else {
        // We don't want to keep polling in a loop since this method is non-blocking.
        break;
      }
    }
    // If we produced a full batch, return it, otherwise finish the current batch and return it.
    return outputBuffer.popAny();
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkNotNull(emitExpressions);

    Schema inputSchema = Objects.requireNonNull(getChild().getSchema());

    ImmutableList.Builder<FlatteningGenericEvaluator<?>> evalBuilder = ImmutableList.builder();
    final ExpressionOperatorParameter parameters = new ExpressionOperatorParameter(inputSchema, getNodeID());
    for (Expression expr : emitExpressions) {
      FlatteningGenericEvaluator<?> evaluator = new FlatteningGenericEvaluator<>(expr, parameters);
      if (evaluator.needsCompiling()) {
        evaluator.compile();
      }
      Preconditions.checkArgument(!evaluator.needsState());
      evalBuilder.add(evaluator);
    }
    emitEvaluators = evalBuilder.build();
    outputBuffer = new TupleBatchBuffer(getSchema());
  }

  @Override
  public Schema generateSchema() {
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

    for (int colIdx : columnsToKeep) {
      typesBuilder.add(inputSchema.getColumnType(colIdx));
      namesBuilder.add(inputSchema.getColumnName(colIdx));
    }
    for (Expression expr : emitExpressions) {
      typesBuilder.add(expr.getOutputType(new ExpressionOperatorParameter(inputSchema)));
      namesBuilder.add(expr.getOutputName());
    }
    return new Schema(typesBuilder.build(), namesBuilder.build());
  }
}
