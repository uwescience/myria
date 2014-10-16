package edu.washington.escience.myria.operator.agg;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). This variant supports aggregates over
 * multiple columns, group by multiple columns.
 * 
 * @see Aggregate
 * @see SingleGroupByAggregate
 */
public final class MultiGroupByAggregate extends UnaryOperator {

  /** Java requires this. **/
  private static final long serialVersionUID = 1L;

  /** Holds the distinct grouping keys. */
  private transient TupleBuffer groupKeys;
  /** Final group keys. */
  private List<TupleBatch> groupKeyList;
  /** Holds the corresponding aggregators for each group key in {@link #groupKeys}. */
  private transient List<Aggregator[]> groupAggs;
  /** Maps the hash of a grouping key to a list of indices in {@link #groupKeys}. */
  private transient TIntObjectMap<TIntList> groupKeyMap;
  /** The schema of the columns indicated by the group keys. */
  private Schema groupSchema;
  /** The schema of the aggregation result. */
  private Schema aggSchema;
  /** A cache of the child's Schema. */
  private Schema inputSchema;

  /** Factories to make the Aggregators. **/
  private final AggregatorFactory[] factories;
  /** Group fields. **/
  private final int[] gfields;
  /** An array [0, 1, .., gfields.length-1] used for comparing tuples. */
  private final int[] grpRange;

  /**
   * Groups the input tuples according to the specified grouping fields, then produces the specified aggregates.
   * 
   * @param child The Operator that is feeding us tuples.
   * @param gfields The columns over which we are grouping the result.
   * @param factories The factories that will produce the {@link Aggregator}s for each group..
   */
  public MultiGroupByAggregate(@Nullable final Operator child, final int[] gfields,
      final AggregatorFactory... factories) {
    super(child);
    this.gfields = Objects.requireNonNull(gfields, "gfields");
    this.factories = Objects.requireNonNull(factories, "factories");
    Preconditions.checkArgument(gfields.length > 1, "to use MultiGroupByAggregate, must group over multiple fields");
    Preconditions.checkArgument(factories.length != 0, "to use MultiGroupByAggregate, must specify some aggregates");
    grpRange = new int[gfields.length];
    for (int i = 0; i < gfields.length; ++i) {
      grpRange[i] = i;
    }
    groupKeyList = null;
  }

  @Override
  protected void cleanup() throws DbException {
    groupKeys = null;
    groupAggs = null;
    groupKeyMap = null;
    groupKeyList = null;
  }

  /**
   * Returns the next tuple. If there is a group by field, then the first field is the field by which we are grouping,
   * and the second field is the result of computing the aggregate, If there is no group by field, then the result tuple
   * should contain one field representing the result of the aggregate. Should return null if there are no more tuples.
   * 
   * @throws DbException if any error occurs.
   * @return result TB.
   */
  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();

    if (child.eos()) {
      return getResultBatch();
    }

    TupleBatch tb = child.nextReady();
    while (tb != null) {
      for (int row = 0; row < tb.numTuples(); ++row) {
        int rowHash = HashUtils.hashSubRow(tb, gfields, row);
        TIntList hashMatches = groupKeyMap.get(rowHash);
        if (hashMatches == null) {
          hashMatches = newKey(rowHash);
          newGroup(tb, row, hashMatches);
          continue;
        }
        TIntIterator matches = hashMatches.iterator();
        boolean found = false;
        while (!found && matches.hasNext()) {
          int curGrp = matches.next();
          if (TupleUtils.tupleEquals(tb, gfields, row, groupKeys, grpRange, curGrp)) {
            updateGroup(tb, row, groupAggs.get(curGrp));
            found = true;
          }
        }
        if (!found) {
          newGroup(tb, row, hashMatches);
        }
        Preconditions.checkState(groupKeys.numTuples() == groupAggs.size());
      }
      tb = child.nextReady();
    }

    /*
     * We know that child.nextReady() has returned <code>null</code>, so we have processed all tuple we can. Child is
     * either EOS or we have to wait for more data.
     */
    if (child.eos()) {
      return getResultBatch();
    }

    return null;
  }

  /**
   * Since row <code>row</code> in {@link TupleBatch} <code>tb</code> does not appear in {@link #groupKeys}, create a
   * new group for it.
   * 
   * @param tb the source {@link TupleBatch}
   * @param row the row in <code>tb</code> that contains the new group
   * @param hashMatches the list of all rows in the output {@link TupleBuffer}s that match this hash.
   * @throws DbException if there is an error.
   */
  private void newGroup(final TupleBatch tb, final int row, final TIntList hashMatches) throws DbException {
    int newIndex = groupKeys.numTuples();
    for (int column = 0; column < gfields.length; ++column) {
      TupleUtils.copyValue(tb, gfields[column], row, groupKeys, column);
    }
    hashMatches.add(newIndex);
    Aggregator[] curAggs = AggUtils.allocateAggs(factories, inputSchema);
    groupAggs.add(curAggs);
    updateGroup(tb, row, curAggs);
    Preconditions.checkState(groupKeys.numTuples() == groupAggs.size(), "groupKeys %s != groupAggs %s", groupKeys
        .numTuples(), groupAggs.size());
  }

  /**
   * Called when there is no list yet of which output aggregators match the specified hash. Creates a new int list to
   * store these matches, and insert it into the {@link #groupKeyMap}.
   * 
   * @param groupHash the hash of the grouping columns in a tuple
   * @return the new (empty still) int list storing which output aggregators match the specified hash
   */
  private TIntList newKey(final int groupHash) {
    TIntList matches = new TIntArrayList(1);
    groupKeyMap.put(groupHash, matches);
    return matches;
  }

  /**
   * Update the aggregators with the tuples in the specified row.
   * 
   * @param tb the source {@link TupleBatch}
   * @param row the row in <code>tb</code> that contains the new values
   * @param curAggs the aggregators to be updated.
   * @throws DbException if there is an error.
   */
  private void updateGroup(final TupleBatch tb, final int row, final Aggregator[] curAggs) throws DbException {
    for (Aggregator agg : curAggs) {
      agg.addRow(tb, row);
    }
  }

  /**
   * @return A batch's worth of result tuples from this aggregate.
   * @throws DbException if there is an error.
   */
  private TupleBatch getResultBatch() throws DbException {
    Preconditions.checkState(getChild().eos(), "cannot extract results from an aggregate until child has reached EOS");
    if (groupKeyList == null) {
      groupKeyList = Lists.newLinkedList(groupKeys.finalResult());
    }

    if (groupKeyList.isEmpty()) {
      return null;
    }

    TupleBatch curGroupKeys = groupKeyList.remove(0);
    TupleBatchBuffer curGroupAggs = new TupleBatchBuffer(aggSchema);
    for (int row = 0; row < curGroupKeys.numTuples(); ++row) {
      Aggregator[] rowAggs = groupAggs.get(row);
      int curCol = 0;
      for (Aggregator rowAgg : rowAggs) {
        rowAgg.getResult(curGroupAggs, curCol);
        curCol += rowAgg.getResultSchema().numColumns();
      }
    }
    TupleBatch aggResults = curGroupAggs.popAny();
    Preconditions.checkState(curGroupKeys.numTuples() == aggResults.numTuples(),
        "curGroupKeys size %s != aggResults size %s", curGroupKeys.numTuples(), aggResults.numTuples());

    /* Note: as of Java7 sublists of sublists do what we want -- the sublists are at most one deep. */
    groupAggs = groupAggs.subList(curGroupKeys.numTuples(), groupAggs.size());
    return new TupleBatch(getSchema(), ImmutableList.<Column<?>> builder().addAll(curGroupKeys.getDataColumns())
        .addAll(aggResults.getDataColumns()).build());
  }

  /**
   * The schema of the aggregate output. Grouping fields first and then aggregate fields. The aggregate
   * 
   * @return the resulting schema
   */
  @Override
  public Schema generateSchema() {
    if (inputSchema == null) {
      Operator child = getChild();
      if (child == null) {
        return null;
      }
      inputSchema = child.getSchema();
      if (inputSchema == null) {
        return null;
      }
    }

    groupSchema = inputSchema.getSubSchema(gfields);

    /* Build the output schema from the group schema and the aggregates. */
    final ImmutableList.Builder<Type> aggTypes = ImmutableList.<Type> builder();
    final ImmutableList.Builder<String> aggNames = ImmutableList.<String> builder();

    for (AggregatorFactory f : factories) {
      Schema curAggSchema = f.getResultSchema(inputSchema);
      aggTypes.addAll(curAggSchema.getColumnTypes());
      aggNames.addAll(curAggSchema.getColumnNames());
    }
    aggSchema = new Schema(aggTypes, aggNames);
    return Schema.merge(groupSchema, aggSchema);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Objects.requireNonNull(getSchema(), "schema");
    groupKeys = new TupleBuffer(groupSchema);
    groupAggs = new ArrayList<>();
    groupKeyMap = new TIntObjectHashMap<>();
  }
}
