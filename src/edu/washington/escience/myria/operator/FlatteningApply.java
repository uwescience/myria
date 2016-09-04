package edu.washington.escience.myria.operator;

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
import com.google.common.collect.Ordering;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.FlatteningGenericEvaluator;
import edu.washington.escience.myria.expression.evaluate.PythonUDFFlatteningEvaluator;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleBuffer;
import edu.washington.escience.myria.storage.TupleUtils;

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
  private ImmutableList<FlatteningGenericEvaluator> emitEvaluators = ImmutableList.of();

  /**
   * Buffer to hold finished and in-progress TupleBatches.
   */
  private TupleBatchBuffer outputBuffer;

  /**
   * Indexes of columns from input relation that we should include in the result (with values duplicated for each result
   * in each expression evaluation). Must be an empty set if no columns are to be retained.
   */
  @Nonnull
  private ImmutableList<Integer> columnsToKeep = ImmutableList.of();

  /**
   * @return the {@link #emitExpressions}
   */
  protected ImmutableList<Expression> getEmitExpressions() {
    return emitExpressions;
  }

  /**
   * @return the {@link #emitEvaluators}
   */
  public List<FlatteningGenericEvaluator> getEmitEvaluators() {
    return emitEvaluators;
  }

  /**
   * The logger for debug, trace, etc. messages in this class.
   */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(FlatteningApply.class);

  /**
   *
   * @param child child operator that data is fetched from
   * @param emitExpressions expression that created the output
   * @param columnsToKeep indexes of columns to keep from input relation
   */
  public FlatteningApply(final Operator child, @Nonnull final List<Expression> emitExpressions,
      final List<Integer> columnsToKeep) {
    super(child);
    Preconditions.checkNotNull(emitExpressions);
    setEmitExpressions(emitExpressions);
    setColumnsToKeep(columnsToKeep);
  }

  /**
   *
   * @param child child operator that data is fetched from
   * @param emitExpressions expression that created the output
   */
  public FlatteningApply(final Operator child, @Nonnull final List<Expression> emitExpressions) {
    this(child, emitExpressions, null);
  }

  /**
   * @param emitExpressions the emit expressions for each column
   */
  private void setEmitExpressions(@Nonnull final List<Expression> emitExpressions) {
    this.emitExpressions = ImmutableList.copyOf(emitExpressions);
  }

  /**
   * @param columnsToKeep indexes of columns to keep from input relation
   */
  private void setColumnsToKeep(final List<Integer> columnsToKeep) {
    if (columnsToKeep != null) {
      boolean sorted = Ordering.natural().isStrictlyOrdered(columnsToKeep);
      Preconditions.checkArgument(sorted, "List of retained columns {} must be sorted and contain no duplicates.",
          columnsToKeep);
      this.columnsToKeep = ImmutableList.copyOf(columnsToKeep);
    }
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException, InvocationTargetException {
    // If there's a batch already finished, return it, otherwise keep reading
    // batches from the child until we have a full batch or the child returns null.
    while (!outputBuffer.hasFilledTB()) {
      TupleBatch inputTuples = getChild().nextReady();
      if (inputTuples != null) {
        // Evaluate expressions on each column and store counts and results.
        List<Column<?>> resultCountColumns = Lists.newLinkedList();
        List<TupleBuffer> evalResultBuffers = new ArrayList<>();
        for (final FlatteningGenericEvaluator evaluator : emitEvaluators) {
          TupleBuffer results = new TupleBuffer(Schema.ofFields(evaluator.getOutputName(), evaluator.getOutputType()));
          Column<?> counts = evaluator.evaluateColumn(inputTuples, results);
          // LOGGER.info("number of results in this column " + counts.size());
          resultCountColumns.add(counts);
          evalResultBuffers.add(results);
        }
        // LOGGER.info("emit.eval size: " + emitEvaluators.size());

        int[] resultCounts = new int[emitEvaluators.size()];
        int[] cumResultCounts = new int[emitEvaluators.size()];
        int[] lastCumResultCounts = new int[emitEvaluators.size()];
        int[] iteratorIndexes = new int[emitEvaluators.size()];

        List<Type> types = Lists.newLinkedList();
        types.add(Type.INT_TYPE);
        List<String> names = ImmutableList.of("flatmapid");
        Schema countIdxSchema = new Schema(types, names);

        for (int rowIdx = 0; rowIdx < inputTuples.numTuples(); ++rowIdx) {
          // First, get all result counts for this row.
          boolean emptyProduct = false;
          for (int i = 0; i < resultCountColumns.size(); ++i) {
            int resultCount = resultCountColumns.get(i).getInt(rowIdx);
            // LOGGER.info("result count for this row: " + resultCount);
            resultCounts[i] = resultCount;
            lastCumResultCounts[i] = cumResultCounts[i];
            cumResultCounts[i] += resultCounts[i];
            if (resultCount == 0) {
              // If at least one evaluator returned zero results, the Cartesian product is empty.
              // LOGGER.info("resultcount is zero!!");
              emptyProduct = true;
            }
          }

          if (!emptyProduct) {
            // Initialize each iterator to its starting index.
            Arrays.fill(iteratorIndexes, 0);
            // int a = 0;
            // Iterate over each element of the Cartesian product and append to output.
            do {
              for (int iteratorIdx = 0; iteratorIdx < iteratorIndexes.length; ++iteratorIdx) {
                int outputColIdx = columnsToKeep.size() + iteratorIdx;
                int resultRowIdx = lastCumResultCounts[iteratorIdx] + iteratorIndexes[iteratorIdx];
                // LOGGER.info("resultRowIdx " + resultRowIdx);
                // LOGGER.info("round " + a);
                // LOGGER.info("iteratorIdx :" + iteratorIdx);

                outputBuffer.appendFromColumn(outputColIdx, evalResultBuffers.get(iteratorIdx).asColumn(0),
                    resultRowIdx);
                TupleBuffer countIdx = new TupleBuffer(countIdxSchema);

                for (int i = 0; i < resultCounts[iteratorIdx]; i++) {
                  countIdx.putInt(0, i);
                }

                outputBuffer.appendFromColumn(outputColIdx + 1, countIdx.asColumn(0), resultRowIdx);
              }

              // Duplicate the values of all columns we are keeping from the original relation in this row.
              int colIdx = 0;
              for (int colKeepIdx : columnsToKeep) {
                TupleUtils.copyValue(inputTuples.asColumn(colKeepIdx), rowIdx, outputBuffer, colIdx++);
              }
              // a = a + 1;
            } while (!computeNextCombination(resultCounts, iteratorIndexes));
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
   *          where each set is of the form [0, i), where i is an element of {@link upperBounds}
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
        // LOGGER.info("reset iterator index to new val");

        iteratorIndexes[iteratorPos] += 1;
        // LOGGER.info("new iterator index" + iteratorIndexes[iteratorPos]);

        break;
      } else {
        // If the outermost iterator is exhausted, we are done.
        if (iteratorPos == 0) {
          endOfIteration = true;
          break;
        } else {
          // Reset the current iterator and continue.
          // LOGGER.info("reset the iterator because reached end");
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

    ImmutableList.Builder<FlatteningGenericEvaluator> evalBuilder = ImmutableList.builder();
    final ExpressionOperatorParameter parameters = new ExpressionOperatorParameter(inputSchema, getNodeID());
    for (Expression expr : emitExpressions) {
      FlatteningGenericEvaluator evaluator;
      if (expr.isRegisteredUDF()) {

        evaluator = new PythonUDFFlatteningEvaluator(expr, parameters, getPythonFunctionRegistrar());
      } else {
        evaluator = new FlatteningGenericEvaluator(expr, parameters);
      }
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
      typesBuilder.add(Type.INT_TYPE);
      namesBuilder.add(expr.getOutputName());
      namesBuilder.add("flatmapid");
    }
    return new Schema(typesBuilder.build(), namesBuilder.build());
  }
}
