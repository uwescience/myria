/**
 *
 */
package edu.washington.escience.myria.operator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;
import edu.washington.escience.myria.operator.network.distribute.HashDistributeFunction;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.parallel.RelationWriteMetadata;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.MyriaUtils;

/** @author valmeida */
public class DbInsertMultiPartition extends AbstractDbInsert {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The connection to the database database. */
  private AccessMethod accessMethod;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;
  /** The name of the table the tuples should be inserted into. */
  private final RelationKey relationKey;
  /** Whether to overwrite an existing table or not. */
  private final boolean overwriteTable;
  /** The name of the table the tuples should be inserted into. */
  private RelationKey tempRelationKey;
  /** The indexes to be created on the table. Each entry is a list of columns. */
  private final List<List<IndexRef>> indexes;
  /** The DistributeFunction used to distribute the table across workers. */
  private final DistributeFunction distributeFunction;

  private int numPartitions;

  /** Constructs an insertion operator to store the tuples from the specified child into the specified database. If the
   * table does not exist, it will be created; if it does exist then old data will persist and new data will be
   * inserted.
   *
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection. */
  public DbInsertMultiPartition(
      final Operator child, final RelationKey relationKey, final ConnectionInfo connectionInfo) {
    this(child, relationKey, connectionInfo, false);
  }

  /** Constructs an insertion operator to store the tuples from the specified child into the worker's default database.
   * If the table does not exist, it will be created. If <code>overwriteTable</code> is <code>true</code>, any existing
   * data will be dropped.
   *
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param overwriteTable whether to overwrite a table that already exists. */
  public DbInsertMultiPartition(
      final Operator child, final RelationKey relationKey, final boolean overwriteTable) {
    this(child, relationKey, null, overwriteTable);
  }

  /** Constructs an insertion operator to store the tuples from the specified child into the specified database. If the
   * table does not exist, it will be created. If <code>overwriteTable</code> is <code>true</code>, any existing data
   * will be dropped.
   *
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param overwriteTable whether to overwrite a table that already exists.
   * @param indexes indexes created. */
  public DbInsertMultiPartition(
      final Operator child,
      final RelationKey relationKey,
      final boolean overwriteTable,
      final List<List<IndexRef>> indexes) {
    this(child, relationKey, null, overwriteTable, indexes);
  }

  /** Constructs an insertion operator to store the tuples from the specified child into the specified database. If the
   * table does not exist, it will be created. If <code>overwriteTable</code> is <code>true</code>, any existing data
   * will be dropped.
   *
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   * @param overwriteTable whether to overwrite a table that already exists. */
  public DbInsertMultiPartition(
      final Operator child,
      final RelationKey relationKey,
      final ConnectionInfo connectionInfo,
      final boolean overwriteTable) {
    this(child, relationKey, connectionInfo, overwriteTable, null);
  }

  /** Constructs an insertion operator to store the tuples from the specified child into the specified database. If the
   * table does not exist, it will be created. If <code>overwriteTable</code> is <code>true</code>, any existing data
   * will be dropped.
   *
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   * @param overwriteTable whether to overwrite a table that already exists.
   * @param indexes the indexes to be created on the table. Each entry is a list of columns. */
  public DbInsertMultiPartition(
      final Operator child,
      final RelationKey relationKey,
      final ConnectionInfo connectionInfo,
      final boolean overwriteTable,
      final List<List<IndexRef>> indexes) {
    this(child, relationKey, connectionInfo, overwriteTable, indexes, null);
  }

  /** Constructs an insertion operator to store the tuples from the specified child into the specified database. If the
   * table does not exist, it will be created. If <code>overwriteTable</code> is <code>true</code>, any existing data
   * will be dropped.
   *
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   * @param overwriteTable whether to overwrite a table that already exists.
   * @param indexes the indexes to be created on the table. Each entry is a list of columns.
   * @param distributeFunction the DistributeFunction used to distribute partitions of the table across workers. */
  public DbInsertMultiPartition(
      final Operator child,
      final RelationKey relationKey,
      final ConnectionInfo connectionInfo,
      final boolean overwriteTable,
      final List<List<IndexRef>> indexes,
      final DistributeFunction distributeFunction) {
    super(child);
    Objects.requireNonNull(relationKey, "relationKey");
    this.connectionInfo = connectionInfo;
    this.relationKey = relationKey;
    this.overwriteTable = overwriteTable;
    this.distributeFunction = distributeFunction;
    /* Sanity check arguments -- cannot create an index in append mode. */
    Preconditions.checkArgument(
        overwriteTable || indexes == null || indexes.size() == 0,
        "Cannot create indexes when appending to a relation.");
    /* 1) construct immutable copies of the given indexes.
     * 2) ensure that the index requests are valid:
     *    - lists of column references must be non-null.
     *    - column references are unique per index. */
    if (indexes != null) {
      ImmutableList.Builder<List<IndexRef>> index = ImmutableList.builder();
      for (List<IndexRef> i : indexes) {
        Objects.requireNonNull(i);
        Preconditions.checkArgument(
            i.size() == ImmutableSet.copyOf(i).size(),
            "Column references cannot be repeated in index definition: %s",
            i);
        index.add(ImmutableList.copyOf(i));
      }
      this.indexes = index.build();
    } else {
      this.indexes = ImmutableList.of();
    }
  }

  @Override
  public void cleanup() {
    try {
      if (accessMethod != null) {
        accessMethod.close();
      }
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void consumeTuples(final TupleBatch tupleBatch) throws DbException {
    Objects.requireNonNull(accessMethod, "accessMethod");
    Objects.requireNonNull(tempRelationKey, "tempRelationKey");
    Preconditions.checkArgument(
        tupleBatch.getSchema().equals(getSchema()),
        "tuple schema %s does not match operator schema %s",
        tupleBatch.getSchema(),
        getSchema());
    /* We need to repartition the tuples routed to this worker to distribute them to the correct partition tables. */
    TupleBatch[] tbs = distributeFunction.getPartitionFunction().partition(tupleBatch);
    for (int i = 0; i < tbs.length; ++i) {
      /* We shouldn't have any non-empty partitions that aren't allocated to this worker. */
      assert tbs[i].numTuples() == 0
          || Ints.contains(distributeFunction.getPartitionsForDestination(getNodeID()), i);
      final RelationKey partRelationKey = partitionKeyFromRelationKey(tempRelationKey, i);
      accessMethod.tupleBatchInsert(partRelationKey, tbs[i]);
    }
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {

    setThreshold(TupleUtils.getBatchSize(getSchema()));

    /* check that our DistributeFunction is compatible with elastic mode if enabled */
    boolean enableElasticMode = false;
    if (getExecEnvVars() != null) {
      enableElasticMode =
          MyriaUtils.isTrue(
              (Boolean) getExecEnvVars().get(MyriaConstants.EXEC_ENV_VAR_ELASTIC_MODE));
    }
    Preconditions.checkArgument(
        !enableElasticMode
            || (distributeFunction != null && distributeFunction instanceof HashDistributeFunction),
        "Only hash partitioning is allowed on clusters in elastic mode");

    this.numPartitions = distributeFunction.getNumPartitions();

    /* retrieve connection information from the environment variables, if not already set */
    if (connectionInfo == null && execEnvVars != null) {
      connectionInfo =
          (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }

    if (connectionInfo == null) {
      throw new DbException("Unable to instantiate DbInsert: connection information unknown");
    }

    /* open the database connection */
    accessMethod = AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);

    if (overwriteTable) {
      /* If overwriting, we insert into a temp table and then on success we drop the old and rename. */
      tempRelationKey =
          RelationKey.of(relationKey.getUserName(), "MyriaSysTemp", relationKey.getRelationName());
      /* Drop the temp table, if it exists. */
      accessMethod.dropTableIfExists(tempRelationKey);
    } else {
      /* Otherwise go ahead and write into the same table. */
      tempRelationKey = relationKey;
    }

    final RelationKey[] partitionRelationKeys =
        allPartitionKeysFromRelationKey(tempRelationKey, numPartitions);
    for (final RelationKey partRelationKey : partitionRelationKeys) {
      /* Create the table */
      accessMethod.createTableIfNotExists(partRelationKey, getSchema());
      /* Create indexes. */
      accessMethod.createIndexes(partRelationKey, getSchema(), indexes);
    }
  }

  @Override
  protected void childEOS() throws DbException {
    /* If the child finished, we're done too. If in overwrite mode, drop the existing table and rename. */
    if (overwriteTable) {
      for (int i = 0; i < numPartitions; ++i) {
        final RelationKey partRelationKey = partitionKeyFromRelationKey(relationKey, i);
        final RelationKey partTempRelationKey = partitionKeyFromRelationKey(tempRelationKey, i);
        accessMethod.dropAndRenameTables(partRelationKey, partTempRelationKey);
      }
    }
    /* Create the logical view over partitions for this relation. */
    accessMethod.createUnionView(
        relationKey, allPartitionKeysFromRelationKey(relationKey, numPartitions));
  }

  @Override
  protected void childEOI() throws DbException {}

  /** @return the name of the relation that this operator will write to. */
  public RelationKey getRelationKey() {
    return relationKey;
  }

  @Override
  public Map<RelationKey, RelationWriteMetadata> writeSet() {
    return ImmutableMap.of(
        relationKey,
        new RelationWriteMetadata(
            relationKey, getSchema(), overwriteTable, false, distributeFunction));
  }

  private static RelationKey partitionKeyFromRelationKey(
      final RelationKey relationKey, final int partition) {
    return RelationKey.of(
        relationKey.getUserName(),
        relationKey.getProgramName(),
        relationKey.getRelationName() + "_" + partition);
  }

  private static RelationKey[] allPartitionKeysFromRelationKey(
      final RelationKey relationKey, final int numPartitions) {
    final RelationKey[] partitionRelationKeys = new RelationKey[numPartitions];
    for (int i = 0; i < numPartitions; ++i) {
      partitionRelationKeys[i] = partitionKeyFromRelationKey(relationKey, i);
    }
    return partitionRelationKeys;
  }
}
