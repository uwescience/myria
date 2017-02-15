package edu.washington.escience.myria.operator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ConstantEvaluator;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator.EvaluatorResult;
import edu.washington.escience.myria.expression.evaluate.PythonUDFEvaluator;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleBuffer;

/**
 * Generic apply operator for single- or multivalued expressions.
 */
public class Apply extends UnaryOperator {
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
  private ImmutableList<GenericEvaluator> emitEvaluators = ImmutableList.of();

  /**
   * Buffer to hold finished and in-progress TupleBatches.
   */
  private TupleBatchBuffer outputBuffer;

  /**
   * AddCounter to the returning tuplebatch.
   */
  private Boolean addCounter = false;

  /**
   * @return the {@link #emitExpressions}
   */
  protected ImmutableList<Expression> getEmitExpressions() {
    return emitExpressions;
  }

  /**
   * @return the {@link #emitEvaluators}
   */
  public List<GenericEvaluator> getEmitEvaluators() {
    return emitEvaluators;
  }

  /**
   * @param evaluators the evaluators to set
   */
  public void setEmitEvaluators(final List<GenericEvaluator> evaluators) {
    emitEvaluators = ImmutableList.copyOf(evaluators);
  }

  /**
   * @return if there are no multivalued emit expressions
   */
  private boolean onlySingleValuedExpressions() {
    for (Expression expr : getEmitExpressions()) {
      if (expr.isMultiValued()) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return number of columns that return more than one value for this Apply operator.
   */
  private int numberOfMultiValuedExpressions() {
    int i = 0;
    for (Expression expr : getEmitExpressions()) {
      if (expr.isMultiValued()) {
        i += 1;
      }
    }
    return i;
  }

  /**
   * Should a counter be added?
   * 
   * @return
   */
  private boolean getAddCounter() {
    return (this.addCounter && (numberOfMultiValuedExpressions() == 1));
  }

  private void setAddCounter(Boolean addCounter) {
    this.addCounter = addCounter;
  }

  /**
   * The logger for debug, trace, etc. messages in this class.
   */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Apply.class);

  /**
   * @param child child operator that data is fetched from
   * @param emitExpressions expression that created the output
   */
  public Apply(final Operator child, @Nonnull final List<Expression> emitExpressions) {
    super(child);
    Preconditions.checkNotNull(emitExpressions);
    setEmitExpressions(emitExpressions);
  }

  public Apply(final Operator child, List<Expression> emitExpressions, Boolean addCounter) {
    this(child, emitExpressions);
    if (addCounter != null) {
      setAddCounter(addCounter);
    }
  }

  /**
   * @param emitExpressions the emit expressions for each column
   */
  private void setEmitExpressions(@Nonnull final List<Expression> emitExpressions) {
    this.emitExpressions = ImmutableList.copyOf(emitExpressions);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException, InvocationTargetException, IOException {
    // If there's a batch already finished, return it, otherwise keep reading
    // batches from the child until we have a full batch or the child returns null.
    while (!outputBuffer.hasFilledTB()) {
      TupleBatch inputTuples = getChild().nextReady();
      if (inputTuples != null) {
        if (onlySingleValuedExpressions()) {
          List<List<Column<?>>> tbs = new ArrayList<List<Column<?>>>();
          for (final GenericEvaluator eval : emitEvaluators) {
            EvaluatorResult evalResult = eval.evaluateColumn(inputTuples, getSchema());
            List<Column<?>> cols = evalResult.getResultColumns();
            for (int i = 0; i < cols.size(); ++i) {
              if (tbs.size() <= i) {
                tbs.add(new ArrayList<Column<?>>());
              }
              tbs.get(i).add(cols.get(i));
            }
          }
          for (List<Column<?>> cols : tbs) {
            outputBuffer.absorb(new TupleBatch(getSchema(), cols), true);
          }
        } else {
          // Evaluate expressions on each column and store counts and results.
          List<ReadableColumn> resultCountColumns = new ArrayList<>();
          List<ReadableColumn> resultColumns = new ArrayList<>();
          for (final GenericEvaluator eval : emitEvaluators) {
            EvaluatorResult evalResult = eval.evaluateColumn(inputTuples, getSchema());
            resultCountColumns.add(evalResult.getResultCounts());
            resultColumns.add(evalResult.getResults());
          }

          // Generate the Cartesian product and append to output buffer.
          int[] resultCounts = new int[emitEvaluators.size()];
          int[] cumResultCounts = new int[emitEvaluators.size()];
          int[] lastCumResultCounts = new int[emitEvaluators.size()];
          int[] iteratorIndexes = new int[emitEvaluators.size()];
          List<Type> types = Lists.newLinkedList();
          types.add(Type.INT_TYPE);
          List<String> names = ImmutableList.of(MyriaConstants.FLATMAP_COLUMN_NAME);
          Schema countIdxSchema = new Schema(types, names);

          for (int rowIdx = 0; rowIdx < inputTuples.numTuples(); ++rowIdx) {
            // First, get all result counts for this row.
            boolean emptyProduct = false;

            for (int i = 0; i < resultCountColumns.size(); ++i) {
              int resultCount = resultCountColumns.get(i).getInt(rowIdx);
              resultCounts[i] = resultCount;
              lastCumResultCounts[i] = cumResultCounts[i];
              cumResultCounts[i] += resultCounts[i];
              if (resultCount == 0) {
                // If at least one evaluator returned zero results, the Cartesian product is empty.
                emptyProduct = true;
              }
            }

            if (!emptyProduct) {
              // Initialize each iterator to its starting index.
              Arrays.fill(iteratorIndexes, 0);
              // Iterate over each element of the Cartesian product and append to output
              TupleBuffer countIdx = null;
              int iteratorIdx = 0;
              int resultRowIdx = 0;
              int flatmapid = -1;
              do {
                for (iteratorIdx = 0; iteratorIdx < iteratorIndexes.length; ++iteratorIdx) {
                  resultRowIdx = lastCumResultCounts[iteratorIdx] + iteratorIndexes[iteratorIdx];
                  if (getAddCounter() && flatmapid < iteratorIndexes[iteratorIdx]) {
                    flatmapid = iteratorIndexes[iteratorIdx];
                  }
                  outputBuffer.appendFromColumn(iteratorIdx, resultColumns.get(iteratorIdx), resultRowIdx);
                }

                if (getAddCounter()) {
                  countIdx = new TupleBuffer(countIdxSchema, 1);
                  countIdx.putInt(0, flatmapid);
                  flatmapid = 0;
                  outputBuffer.appendFromColumn(iteratorIdx, countIdx.asColumn(0), 0);
                }
              } while (!computeNextCombination(resultCounts, iteratorIndexes));
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

  /**
   * This method mutates {@link iteratorIndexes} on each call to yield the next element of the Cartesian product of
   * {@link upperBounds} in lexicographic order. If all elements have been exhausted, it returns true, otherwise it
   * returns false.
   *
   * @param upperBounds an immutable array of elements representing the sets we are forming the Cartesian product of,
   *        where each set is of the form [0, i), where i is an element of {@link upperBounds}
   * @param iteratorIndexes a mutable array of elements representing the current element of the Cartesian product
   * @return if we have exhausted all elements of the Cartesian product
   */
  private boolean computeNextCombination(final int[] upperBounds, final int[] iteratorIndexes) {
    boolean endOfIteration = false;
    int lastIteratorPos = iteratorIndexes.length - 1;
    // Count backward from the innermost iterator to the outermost.
    for (int iteratorPos = lastIteratorPos; iteratorPos >= 0; --iteratorPos) {
      // If the current iterator is not exhausted, increment it and exit the loop,
      // otherwise reset the current iterator and continue.
      if (iteratorIndexes[iteratorPos] < upperBounds[iteratorPos] - 1) {
        iteratorIndexes[iteratorPos] += 1;
        break;
      } else {
        // If the outermost iterator is exhausted, we are done.
        if (iteratorPos == 0) {
          endOfIteration = true;
          break;
        } else {
          // Reset the current iterator and continue.
          iteratorIndexes[iteratorPos] = 0;
        }
      }
    }
    return endOfIteration;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkNotNull(emitExpressions);

    Schema inputSchema = Objects.requireNonNull(getChild().getSchema());

    List<GenericEvaluator> evals = new ArrayList<>();
    final ExpressionOperatorParameter parameters = new ExpressionOperatorParameter(inputSchema, null, getNodeID(),
        getPythonFunctionRegistrar());

    for (Expression expr : emitExpressions) {
      GenericEvaluator evaluator;
      if (expr.isConstant()) {
        evaluator = new ConstantEvaluator(expr, parameters);
      } else if (expr.isRegisteredPythonUDF()) {
        evaluator = new PythonUDFEvaluator(expr, parameters);
      } else {
        evaluator = new GenericEvaluator(expr, parameters);
      }
      if (evaluator.needsCompiling()) {
        evaluator.compile();
      }
      Preconditions.checkArgument(!evaluator.needsState());
      evals.add(evaluator);
    }
    setEmitEvaluators(evals);

    outputBuffer = new TupleBatchBuffer(generateSchema());
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

    for (Expression expr : emitExpressions) {
      typesBuilder.add(expr.getOutputType(new ExpressionOperatorParameter(inputSchema)));
      namesBuilder.add(expr.getOutputName());
    }
    if (getAddCounter()) {
      typesBuilder.add(Type.INT_TYPE);
      namesBuilder.add(MyriaConstants.FLATMAP_COLUMN_NAME);
    }
    return new Schema(typesBuilder.build(), namesBuilder.build());
  }
}
