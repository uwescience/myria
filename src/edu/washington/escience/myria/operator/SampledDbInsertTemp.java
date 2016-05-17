/**
 *
 */
package edu.washington.escience.myria.operator;

import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.IntColumnBuilder;
import edu.washington.escience.myria.parallel.RelationWriteMetadata;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Samples the stream into a temp relation.
 */
public class SampledDbInsertTemp extends DbInsertTemp {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The name of the table the tuples should be inserted into. */
  private final RelationKey countRelationKey;

  /** Number of tuples seen so far from the child. */
  private int currentTupleCount;

  /** Number of tuples to sample from the stream. */
  private final int sampleSize;

  /** Reservoir that holds sampleSize number of tuples. */
  private MutableTupleBuffer reservoir;

  /** Random generator used for creating the distribution. */
  private Random rand;

  /** Schema that will be written to the countRelationKey. */
  private static final Schema COUNT_SCHEMA =
      Schema.ofFields(
          "WorkerID",
          Type.INT_TYPE,
          "PartitionSize",
          Type.INT_TYPE,
          "PartitionSampleSize",
          Type.INT_TYPE);

  /**
   *
   * @param child
   *          the source of tuples to be inserted
   * @param sampleSize
   *          number of tuples to store from the stream
   * @param sampleRelationKey
   *          the key of the table that tuples will be inserted into
   * @param countRelationKey
   *          the key of the table that tuple count info will be inserted into
   * @param connectionInfo
   *          parameters of the database connection
   * @param randomSeed
   *          value to seed the random generator with. null if no specified seed
   */
  public SampledDbInsertTemp(
      final Operator child,
      final int sampleSize,
      final RelationKey sampleRelationKey,
      final RelationKey countRelationKey,
      final ConnectionInfo connectionInfo,
      Long randomSeed) {
    super(child, sampleRelationKey, connectionInfo, false, null);
    Preconditions.checkArgument(sampleSize >= 0, "sampleSize must be non-negative");
    this.sampleSize = sampleSize;
    Preconditions.checkNotNull(countRelationKey, "countRelationKey cannot be null");
    this.countRelationKey = countRelationKey;
    rand = new Random();
    if (randomSeed != null) {
      rand.setSeed(randomSeed);
    }
  }

  /**
   * Uses reservoir sampling to insert the specified sampleSize.
   * https://en.wikipedia.org/wiki/Reservoir_sampling
   */
  @Override
  protected void consumeTuples(final TupleBatch tb) throws DbException {
    final List<? extends Column<?>> columns = tb.getDataColumns();
    for (int i = 0; i < tb.numTuples(); i++) {
      if (reservoir.numTuples() < sampleSize) {
        // Reservoir size < k. Add this tuple.
        for (int j = 0; j < tb.numColumns(); j++) {
          reservoir.put(j, columns.get(j), i);
        }
      } else {
        // Replace probabilistically
        int replaceIdx = rand.nextInt(currentTupleCount);
        if (replaceIdx < sampleSize) {
          for (int j = 0; j < tb.numColumns(); j++) {
            reservoir.replace(j, replaceIdx, columns.get(j), i);
          }
        }
      }
      currentTupleCount++;
    }
  }

  @Override
  protected void childEOS() throws DbException {
    // Insert the reservoir samples.
    for (TupleBatch tb : reservoir.getAll()) {
      accessMethod.tupleBatchInsert(getRelationKey(), tb);
    }
    // Insert (WorkerID, PartitionSize, PartitionSampleSize) to
    // countRelationKey.
    IntColumnBuilder wIdCol = new IntColumnBuilder();
    IntColumnBuilder tupCountCol = new IntColumnBuilder();
    IntColumnBuilder sampledSizeCol = new IntColumnBuilder();
    wIdCol.appendInt(getNodeID());
    tupCountCol.appendInt(currentTupleCount);
    sampledSizeCol.appendInt(reservoir.numTuples());
    ImmutableList.Builder<Column<?>> columns = ImmutableList.builder();
    columns.add(wIdCol.build(), tupCountCol.build(), sampledSizeCol.build());
    TupleBatch tb = new TupleBatch(COUNT_SCHEMA, columns.build());
    accessMethod.tupleBatchInsert(countRelationKey, tb);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    setupConnection(execEnvVars);

    // Set up the reservoir table.
    accessMethod.dropTableIfExists(getRelationKey());
    accessMethod.createTableIfNotExists(getRelationKey(), getSchema());

    // Set up the tuple count table.
    accessMethod.dropTableIfExists(countRelationKey);
    accessMethod.createTableIfNotExists(countRelationKey, COUNT_SCHEMA);

    reservoir = new MutableTupleBuffer(getChild().getSchema());
  }

  @Override
  public void cleanup() {
    super.cleanup();
    reservoir = null;
  }

  @Override
  public Map<RelationKey, RelationWriteMetadata> writeSet() {
    return ImmutableMap.of(
        getRelationKey(),
        new RelationWriteMetadata(getRelationKey(), getSchema(), true, true),
        countRelationKey,
        new RelationWriteMetadata(countRelationKey, COUNT_SCHEMA, true, true));
  }
}
