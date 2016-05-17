package edu.washington.escience.myria.operator;

import java.lang.reflect.InvocationTargetException;
import java.util.BitSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.BooleanEvaluator;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Filter is an operator that implements a relational select.
 */
public final class Filter extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * The operator.
   * */
  private final Expression predicate;

  /**
   * Evaluator that evaluates {@link #predicate}.
   */
  private BooleanEvaluator evaluator;

  /**
   * Constructor accepts a predicate to apply and a child operator to read tuples to filter from.
   *
   * @param predicate the predicate by which to filter tuples.
   * @param child The child operator
   */
  public Filter(final Expression predicate, final Operator child) {
    super(child);
    this.predicate = predicate;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    Operator child = getChild();
    for (TupleBatch tb = child.nextReady(); tb != null; tb = child.nextReady()) {
      BitSet bits = new BitSet(tb.numTuples());
      for (int rowIdx = 0; rowIdx < tb.numTuples(); rowIdx++) {
        Boolean valid;
        try {
          valid = evaluator.eval(tb, rowIdx);
        } catch (InvocationTargetException e) {
          throw new DbException(e);
        }
        if (valid) {
          bits.set(rowIdx);
        }
      }

      if (bits.cardinality() == 0) {
        continue;
      }

      return tb.filter(bits);
    }
    return null;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkNotNull(predicate);

    Schema inputSchema = getChild().getSchema();

    final ExpressionOperatorParameter parameters =
        new ExpressionOperatorParameter(inputSchema, getNodeID());

    evaluator = new BooleanEvaluator(predicate, parameters);
    if (evaluator.needsCompiling()) {
      evaluator.compile();
    }
  }

  @Override
  public Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }
}
