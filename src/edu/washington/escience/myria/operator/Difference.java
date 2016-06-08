package edu.washington.escience.myria.operator;

import java.util.BitSet;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;

/**
 * Implementation of set difference. Duplicates are not preserved.
 *
 * This implementation is similar to RightHashJoin: read the right relation into a hash table; probe the left relation's
 * tuples with this hash table; eliminate duplicates by adding the left relation to the hash table.
 *
 * @author whitaker
 */
public final class Difference extends BinaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * This buffer stores tuples to remove from the left operator.
   */
  private transient MutableTupleBuffer tuplesToRemove = null;

  /**
   * Mapping from tuple hash code to indices in the tuplesToRemove buffer.
   * */
  private transient IntObjectHashMap<IntArrayList> tupleIndices;

  /**
   * Instantiate a set difference operator: left EXCEPT right.
   *
   * @param left the operator being subtracted from.
   * @param right the operator to be subtracted.
   */
  public Difference(final Operator left, final Operator right) {
    super(left, right);
  }

  /**
   * Mark a particular tuple as seen.
   *
   * @param batch A tuple batch
   * @param rowNum The index of the tuple among the valid tuples in batch.
   *
   * @return true if this is the first time this tuple has been encountered.
   */
  private boolean markAsSeen(final TupleBatch batch, final int rowNum) {
    final int tupleHash = HashUtils.hashRow(batch, rowNum);

    IntArrayList tupleIndexList = tupleIndices.get(tupleHash);

    if (tupleIndexList == null) {
      tupleIndexList = new IntArrayList();
      tupleIndices.put(tupleHash, tupleIndexList);
    }

    // Check whether we've seen this tuple before
    for (int i = 0; i < tupleIndexList.size(); i++) {
      if (TupleUtils.tupleEquals(batch, rowNum, tuplesToRemove, tupleIndexList.get(i))) {
        return false;
      }
    }

    // This is a new tuple: add it to the toRemove tuple buffer
    final int nextToRemoveRow = tuplesToRemove.numTuples();
    final List<? extends Column<?>> columns = batch.getDataColumns();

    for (int columnNum = 0; columnNum < batch.numColumns(); columnNum++) {
      tuplesToRemove.put(columnNum, columns.get(columnNum), rowNum);
    }
    tupleIndexList.add(nextToRemoveRow);
    return true;
  }

  /**
   * Process a batch of tuples that are removed from the final result.
   *
   * @param batch A tuple batch
   */
  private void processRightChildTB(final TupleBatch batch) {
    final int numValidTuples = batch.numTuples();
    for (int row = 0; row < numValidTuples; row++) {
      markAsSeen(batch, row);
    }
  }

  /**
   * Process a batch of tuples that are subtracted from to produce the final result.
   *
   * @param batch A tuple batch.
   *
   * @return A filtered batch of tuples.
   */
  private TupleBatch processLeftChildTB(final TupleBatch batch) {
    final int numValidTuples = batch.numTuples();
    final BitSet toRemove = new BitSet(numValidTuples);

    for (int row = 0; row < numValidTuples; row++) {
      if (!markAsSeen(batch, row)) {
        toRemove.set(row);
      }
    }

    return batch.filterOut(toRemove);
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    final Operator right = getRight();

    /* Drain the right child. */
    while (!right.eos()) {
      TupleBatch rightTB = right.nextReady();
      if (rightTB == null) {
        if (right.eos()) {
          break;
        }
        return null;
      }
      processRightChildTB(rightTB);
    }

    /* Drain the left child */
    final Operator left = getLeft();
    while (!left.eos()) {
      TupleBatch leftTB = left.nextReady();
      if (leftTB == null) {
        return null;
      }
      return processLeftChildTB(leftTB);
    }

    return null;
  }

  @Override
  protected Schema generateSchema() {
    if (getLeft() == null) {
      return null;
    } else {
      return getLeft().getSchema();
    }
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {

    if (!getLeft().getSchema().compatible(getRight().getSchema())) {
      throw new DbException("Incompatible input schemas");
    }

    tupleIndices = new IntObjectHashMap<>();
    tuplesToRemove = new MutableTupleBuffer(getSchema());
  }

  @Override
  protected void cleanup() throws DbException {
    tuplesToRemove = null;
    tupleIndices = null;
  }
}
