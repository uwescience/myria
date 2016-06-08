package edu.washington.escience.myria.operator;

import java.util.BitSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.gs.collections.api.block.procedure.primitive.IntProcedure;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;

/**
 * Duplicate elimination. It adds newly meet unique tuples into a buffer so that the source TupleBatches are not
 * referenced. This implementation reduces memory consumption.
 * */
public final class DupElim extends StreamingState {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for this class.
   * */
  static final Logger LOGGER = LoggerFactory.getLogger(DupElim.class);

  /**
   * Indices to unique tuples.
   * */
  private transient IntObjectHashMap<IntArrayList> uniqueTupleIndices;

  /**
   * The buffer for storing unique tuples.
   * */
  private transient MutableTupleBuffer uniqueTuples = null;

  @Override
  public void cleanup() {
    uniqueTuples = null;
    uniqueTupleIndices = null;
  }

  /**
   * Do duplicate elimination for tb.
   *
   * @param tb the TupleBatch for performing DupElim.
   * @return the duplicate eliminated TB.
   * */
  protected TupleBatch doDupElim(final TupleBatch tb) {
    final int numTuples = tb.numTuples();
    /* if tb is empty, directly return. */
    if (numTuples <= 0) {
      return tb;
    }
    checkUniqueness.inputTB = tb;
    List<? extends Column<?>> columns = tb.getDataColumns();
    final BitSet toRemove = new BitSet(numTuples);
    for (int i = 0; i < numTuples; ++i) {
      final int nextIndex = uniqueTuples.numTuples();
      final int cntHashCode = HashUtils.hashRow(tb, i);
      IntArrayList tupleIndexList = uniqueTupleIndices.get(cntHashCode);
      checkUniqueness.row = i;
      checkUniqueness.unique = true;
      if (tupleIndexList == null) {
        tupleIndexList = new IntArrayList(1);
        tupleIndexList.add(nextIndex);
        uniqueTupleIndices.put(cntHashCode, tupleIndexList);
      } else {
        tupleIndexList.forEach(checkUniqueness);
      }
      if (checkUniqueness.unique) {
        for (int j = 0; j < tb.numColumns(); ++j) {
          uniqueTuples.put(j, columns.get(j), i);
        }
        tupleIndexList.add(nextIndex);
      } else {
        toRemove.set(i);
      }
    }
    return tb.filterOut(toRemove);
  }

  @Override
  public Schema getSchema() {
    return getOp().getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) {
    uniqueTupleIndices = new IntObjectHashMap<>();
    uniqueTuples = new MutableTupleBuffer(getSchema());
    checkUniqueness = new CheckUniquenessProcedure();
  }

  @Override
  public TupleBatch update(final TupleBatch tb) {
    TupleBatch newtb = doDupElim(tb);
    if (newtb.numTuples() > 0 || tb.isEOI()) {
      return newtb;
    }
    return null;
  }

  @Override
  public List<TupleBatch> exportState() {
    return uniqueTuples.getAll();
  }

  @Override
  public int numTuples() {
    if (uniqueTuples == null) {
      return 0;
    }
    return uniqueTuples.numTuples();
  }

  /**
   * Traverse through the list of tuples.
   * */
  private transient CheckUniquenessProcedure checkUniqueness;

  /**
   * Traverse through the list of tuples with the same hash code.
   * */
  private final class CheckUniquenessProcedure implements IntProcedure {

    /** serialization id. */
    private static final long serialVersionUID = 1L;

    /** row index of the tuple. */
    private int row;

    /** input TupleBatch. */
    private TupleBatch inputTB;

    /** if found a replacement. */
    private boolean unique;

    @Override
    public void value(final int index) {
      if (TupleUtils.tupleEquals(inputTB, row, uniqueTuples, index)) {
        unique = false;
      }
    }
  };

  @Override
  public StreamingState newInstanceFromMyself() {
    return new DupElim();
  }
}
