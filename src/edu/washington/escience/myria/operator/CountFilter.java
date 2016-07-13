package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.gs.collections.api.block.procedure.primitive.IntProcedure;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;

/**
 * Keeps distinct tuples with their counts and only emits a tuple at the first time when its count hits the threshold.
 */
public final class CountFilter extends StreamingState {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for this class.
   */
  static final Logger LOGGER = LoggerFactory.getLogger(CountFilter.class);

  /**
   * Indices to unique tuples.
   */
  private transient IntObjectHashMap<IntArrayList> uniqueTupleIndices;

  /**
   * The buffer for storing unique tuples.
   */
  private transient MutableTupleBuffer uniqueTuples = null;
  /**
   * The count of each unique tuple.
   */
  private transient MutableTupleBuffer tupleCounts = null;

  /** threshold of the count. */
  public final int threshold;

  /** column indices of the key. */
  private final int[] keyColIndices;

  /**
   * @param threshold threshold
   * @param keyColIndices key column indices.
   */
  public CountFilter(final int threshold, final int[] keyColIndices) {
    Preconditions.checkArgument(threshold >= 0, "threshold needs to be greater than or equal to 0");
    this.threshold = threshold;
    this.keyColIndices = Arrays.copyOf(keyColIndices, keyColIndices.length);
  }

  @Override
  public void cleanup() {
    uniqueTuples = null;
    uniqueTupleIndices = null;
    tupleCounts = null;
  }

  /**
   * Do duplicate elimination for tb.
   *
   * @param tb the TupleBatch for performing DupElim.
   * @return the duplicate eliminated TB.
   */
  protected TupleBatch countFilter(final TupleBatch tb) {
    final int numTuples = tb.numTuples();
    if (numTuples <= 0) {
      return tb;
    }
    doCount.inputTB = tb;
    final List<? extends Column<?>> columns = tb.getDataColumns();
    final BitSet toRemove = new BitSet(numTuples);
    for (int i = 0; i < numTuples; ++i) {
      final int nextIndex = uniqueTuples.numTuples();
      final int cntHashCode = HashUtils.hashSubRow(tb, keyColIndices, i);
      IntArrayList tupleIndexList = uniqueTupleIndices.get(cntHashCode);
      if (tupleIndexList == null) {
        tupleIndexList = new IntArrayList();
        uniqueTupleIndices.put(cntHashCode, tupleIndexList);
      }
      doCount.found = false;
      doCount.meet = false;
      doCount.sourceRow = i;
      tupleIndexList.forEach(doCount);
      if (!doCount.found) {
        for (int j : keyColIndices) {
          uniqueTuples.put(j, columns.get(j), i);
        }
        tupleCounts.putInt(0, 1);
        tupleIndexList.add(nextIndex);
        if (threshold <= 1) {
          doCount.meet = true;
        }
      }
      if (!doCount.meet) {
        toRemove.set(i);
      }
    }
    return tb.filterOut(toRemove).selectColumns(keyColIndices);
  }

  @Override
  public Schema getSchema() {
    Schema schema = getOp().getInputSchema();
    if (schema == null) {
      return null;
    }
    return schema.getSubSchema(keyColIndices);
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) {
    uniqueTupleIndices = new IntObjectHashMap<IntArrayList>();
    uniqueTuples = new MutableTupleBuffer(getSchema());
    tupleCounts = new MutableTupleBuffer(Schema.ofFields(Type.INT_TYPE, "count"));
    doCount = new CountProcedure();
  }

  @Override
  public TupleBatch update(final TupleBatch tb) {
    TupleBatch newtb = countFilter(tb);
    if (newtb.numTuples() > 0 || newtb.isEOI()) {
      return newtb;
    }
    return null;
  }

  @Override
  public List<TupleBatch> exportState() {
    List<TupleBatch> tbs = uniqueTuples.getAll();
    List<TupleBatch> ret = new ArrayList<TupleBatch>();
    int row = 0;
    for (TupleBatch tb : tbs) {
      final BitSet toRemove = new BitSet(tb.numTuples());
      for (int i = 0; i < tb.numTuples(); ++i) {
        if (tupleCounts.getInt(0, row) < threshold) {
          toRemove.set(i);
        }
        row++;
      }
      ret.add(tb.filterOut(toRemove));
    }
    return ret;
  }

  /**
   * Traverse through the list of tuples and replace old values.
   */
  private transient CountProcedure doCount;

  /**
   * Traverse through the list of tuples with the same hash code.
   */
  private final class CountProcedure implements IntProcedure {

    /** row index of the tuple. */
    private int sourceRow;

    /** input TupleBatch. */
    private TupleBatch inputTB;

    /** if found a key. */
    private boolean found;

    /** if meet the threshold for the first time. */
    private boolean meet;

    @Override
    public void value(final int destRow) {
      if (TupleUtils.tupleEquals(inputTB, keyColIndices, sourceRow, uniqueTuples, destRow)) {
        found = true;
        int newcount = tupleCounts.getInt(0, destRow) + 1;
        if (newcount <= threshold) {
          tupleCounts.replaceInt(0, destRow, newcount);
        }
        meet = (newcount >= threshold);
      }
    }
  };

  @Override
  public int numTuples() {
    if (uniqueTuples == null) {
      return 0;
    }
    return uniqueTuples.numTuples();
  }

  @Override
  public StreamingState duplicate() {
    return new CountFilter(threshold, keyColIndices);
  }
}
