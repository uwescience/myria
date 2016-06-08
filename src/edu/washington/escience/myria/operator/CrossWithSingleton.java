package edu.washington.escience.myria.operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ConstantValueColumn;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Given a singleton right child, cross the left child with it in a way that minimizes state and does not construct new
 * tuples.
 */
public class CrossWithSingleton extends BinaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The singleton tuple from the right child. */
  private TupleBatch rightTuple;

  /**
   * Instantiate a new operator to cross all tuples in the left child with the singleton tuple from the right child.
   *
   * @param left the left child, which may have any number of tuples.
   * @param right the right child, which may only have one tuple.
   */
  public CrossWithSingleton(final Operator left, final Operator right) {
    super(left, right);
    rightTuple = null;
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {

    /* Before we can do anything, get the singleton tuple from the right child, and ensure that it is a singleton. */
    Operator right = getRight();
    while (!right.eos()) {
      TupleBatch tb = right.nextReady();
      if (tb == null) {
        /* The right child may have realized it's EOS now. If so, we must move onto left child to avoid livelock. */
        if (right.eos()) {
          break;
        }
        return null;
      }
      Preconditions.checkState(
          rightTuple == null,
          "Expecting a singleton right child, but received a batch with %s additional tuples",
          tb.numTuples());
      Preconditions.checkState(
          tb.numTuples() == 1,
          "Expecting a singleton right child, instead received a batch with %s tuples",
          tb.numTuples());
      rightTuple = tb;
    }

    /* Verify that the right child did produce a tuple. */
    Preconditions.checkState(
        rightTuple != null,
        "Expecting a singleton right child, but right child is EOS and no tuples received.");

    Operator left = getLeft();
    Schema schema = getSchema();
    while (!left.eos()) {
      TupleBatch tb = left.nextReady();
      if (tb == null) {
        break;
      }

      ImmutableList.Builder<Column<?>> columns = ImmutableList.builder();
      for (Column<?> c : tb.getDataColumns()) {
        columns.add(c);
      }
      for (Column<?> c : rightTuple.getDataColumns()) {
        columns.add(new ConstantValueColumn(c.getObject(0), c.getType(), tb.numTuples()));
      }
      return new TupleBatch(schema, columns.build());
    }

    return null;
  }

  @Override
  protected Schema generateSchema() {
    Operator left = getLeft();
    Operator right = getRight();
    if (left == null || right == null) {
      return null;
    }

    Schema leftSchema = left.getSchema();
    Schema rightSchema = right.getSchema();
    if (leftSchema == null || rightSchema == null) {
      return null;
    }

    return Schema.merge(leftSchema, rightSchema);
  }
}
