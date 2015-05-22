/**
 * 
 */
package edu.washington.escience.myria.operator;

import java.io.File;
import java.util.*;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.*;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.IntColumnBuilder;
import edu.washington.escience.myria.parallel.RelationWriteMetadata;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Samples the stream into a temp relation.
 */
public class SampledDbInsertTemp extends UnaryOperator implements DbWriter {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The connection to the database database. */
  private AccessMethod accessMethod;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;
  /** The name of the table the tuples should be inserted into. */
  private final RelationKey sampleRelationKey;
  /** The name of the table the tuples should be inserted into. */
  private final RelationKey countRelationKey;

  /** Total number of tuples seen from the child. */
  private int tupleCount = 0;
  /** Number of tuples to sample from the stream. */
  private final int streamSampleSize;
  /** Reservoir that holds sampleSize number of tuples. */
  private MutableTupleBuffer reservoir = null;
  /** Sampled tuples ready to be returned. */
  private List<TupleBatch> batches;
  /** Next element of batches List that will be returned. */
  private int batchNum = 0;
  /** True if all samples have been gathered from the child. */
  private boolean doneSamplingFromChild;

  /** The output schema. */
  private static final Schema COUNT_SCHEMA = Schema.of(
      ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE),
      ImmutableList.of("WorkerID", "PartitionSize", "StreamSize"));

  /**
   * @param child
   *          the source of tuples to be inserted.
   * @param streamSampleSize
   *          number of tuples to store from the stream
   * @param sampleRelationKey
   *          the key of the table that the tuples should be inserted into.
   * @param countRelationKey
   *          the key of the table that the tuple counts will be inserted into.
   * @param connectionInfo
   *          the parameters of the database connection.
   */
  public SampledDbInsertTemp(final Operator child, final int streamSampleSize,
      final RelationKey sampleRelationKey, final RelationKey countRelationKey,
      final ConnectionInfo connectionInfo) {
    super(child);
    // Sampling setup.
    Preconditions.checkArgument(streamSampleSize >= 0L,
        "sampleSize must be non-negative");
    this.streamSampleSize = streamSampleSize;
    doneSamplingFromChild = false;

    // Relation setup.
    Objects.requireNonNull(sampleRelationKey, "sampleRelationKey");
    this.sampleRelationKey = sampleRelationKey;
    Objects.requireNonNull(countRelationKey, "countRelationKey");
    this.countRelationKey = countRelationKey;
    this.connectionInfo = connectionInfo;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    if (!doneSamplingFromChild) {
      fillReservoir();
      batches = reservoir.getAll();
      // Insert sampled tuples into sampleRelationKey
      while (batchNum < batches.size()) {
        TupleBatch batch = batches.get(batchNum);
        accessMethod.tupleBatchInsert(sampleRelationKey, batch);
        batchNum++;
      }

      // Write (WorkerID, PartitionSize, StreamSize) to countRelationKey
      IntColumnBuilder wIdCol = new IntColumnBuilder();
      IntColumnBuilder tupCountCol = new IntColumnBuilder();
      IntColumnBuilder streamSizeCol = new IntColumnBuilder();
      wIdCol.appendInt(getNodeID());
      tupCountCol.appendInt(tupleCount);
      streamSizeCol.appendInt(reservoir.numTuples());
      ImmutableList.Builder<Column<?>> columns = ImmutableList.builder();
      columns.add(wIdCol.build(), tupCountCol.build(), streamSizeCol.build());
      TupleBatch tb = new TupleBatch(COUNT_SCHEMA, columns.build());
      accessMethod.tupleBatchInsert(countRelationKey, tb);
    }
    return null;
  }

  /**
   * Fills reservoir with child tuples.
   *
   * @throws DbException
   *           if TupleBatch fails to get nextReady
   */
  private void fillReservoir() throws DbException {
    Random rand = new Random();
    for (TupleBatch tb = getChild().nextReady(); tb != null; tb = getChild()
        .nextReady()) {
      final List<? extends Column<?>> columns = tb.getDataColumns();
      for (int i = 0; i < tb.numTuples(); i++) {
        if (reservoir.numTuples() < streamSampleSize) {
          // Reservoir size < k. Add this tuple.
          for (int j = 0; j < tb.numColumns(); j++) {
            reservoir.put(j, columns.get(j), i);
          }
        } else {
          // Replace probabilistically
          int replaceIdx = rand.nextInt(tupleCount);
          if (replaceIdx < reservoir.numTuples()) {
            for (int j = 0; j < tb.numColumns(); j++) {
              reservoir.replace(j, replaceIdx, columns.get(j), i);
            }
          }
        }
        tupleCount++;
      }
    }
    doneSamplingFromChild = true;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars)
      throws DbException {
    reservoir = new MutableTupleBuffer(getChild().getSchema());
    /*
     * retrieve connection information from the environment variables, if not
     * already set
     */
    if (connectionInfo == null && execEnvVars != null) {
      connectionInfo = (ConnectionInfo) execEnvVars
          .get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }

    if (connectionInfo == null) {
      throw new DbException(
          "Unable to instantiate SampledDbInsertTemp: connection information unknown");
    }

    if (connectionInfo instanceof SQLiteInfo) {
      /* Set WAL in the beginning. */
      final File dbFile = new File(
          ((SQLiteInfo) connectionInfo).getDatabaseFilename());
      SQLiteConnection conn = new SQLiteConnection(dbFile);
      try {
        conn.open(true);
        conn.exec("PRAGMA journal_mode=WAL;");
      } catch (SQLiteException e) {
        e.printStackTrace();
      }
      conn.dispose();
    }

    /* open the database connection */
    accessMethod = AccessMethod.of(connectionInfo.getDbms(), connectionInfo,
        false);
    accessMethod.dropTableIfExists(sampleRelationKey);
    accessMethod.dropTableIfExists(countRelationKey);
    // Create the temp tables.
    accessMethod.createTableIfNotExists(sampleRelationKey, getSchema());
    accessMethod.createTableIfNotExists(countRelationKey, COUNT_SCHEMA);
  }

  @Override
  public void cleanup() {
    reservoir = null;
    batches = null;

    try {
      if (accessMethod != null) {
        accessMethod.close();
      }
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final Schema generateSchema() {
    if (getChild() == null) {
      return null;
    }
    return getChild().getSchema();
  }

  @Override
  public Map<RelationKey, RelationWriteMetadata> writeSet() {
    Map<RelationKey, RelationWriteMetadata> map = new HashMap<RelationKey, RelationWriteMetadata>(2);
    map.put(sampleRelationKey, new RelationWriteMetadata(sampleRelationKey, getSchema(), true, true));
    map.put(countRelationKey, new RelationWriteMetadata(countRelationKey, COUNT_SCHEMA, true, true));
    return map;
  }

}
