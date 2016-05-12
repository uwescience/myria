/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.FixValuePartitionFunction;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Methods to help prepare the data for PSLA generation
 */
public class PerfEnforceDataPreparation {

  private final Server server;

  public PerfEnforceDataPreparation(final Server server) {
    this.server = server;
  }

  /*
   * Ingesting the fact table in a parallel sequence
   */
  public void ingestFact(final RelationKey relationKey, final DataSource source, final Schema schema,
      final Character delimiter, final Set<Integer> configurations) {
    ArrayList<RelationKey> relationKeysToUnion = new ArrayList<RelationKey>();
    ArrayList<Integer> configs = new ArrayList<Integer>(configurations);
    Collections.sort(configs, Collections.reverseOrder());

    // Create a sequence for the largest cluster size
    int maxConfig = configs.get(0);
    Set<Integer> rangeMax = PerfEnforceUtils.getRangeSet(maxConfig);

    // Ingest for the largest cluster size
    RelationKey maxConfigRelationKeyToUnion =
        new RelationKey(relationKey.getUserName(), relationKey.getProgramName(), relationKey.getRelationName()
            + maxConfig + "_U");

    try {
      server.ingestCSVDatasetInParallel(maxConfigRelationKeyToUnion, source, schema, delimiter, rangeMax);
      relationKeysToUnion.add(maxConfigRelationKeyToUnion);

      RelationKey maxConfigRelationKey =
          new RelationKey(relationKey.getUserName(), relationKey.getProgramName(), relationKey.getRelationName()
              + maxConfig);

      server.createView(maxConfigRelationKey.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL), PerfEnforceUtils
          .createUnionQuery(relationKeysToUnion), rangeMax);

      server.addDatasetToCatalog(maxConfigRelationKey, schema, rangeMax);
    } catch (DbException | InterruptedException | CatalogException e1) {
      e1.printStackTrace();
    }

    // Iterate for moving and set parameters
    Set<Integer> previousRange = rangeMax;
    RelationKey previousRelationKey = maxConfigRelationKeyToUnion;
    for (int c = 1; c < configs.size(); c++) {
      // get the new worker sequence
      int currentSize = configs.get(c);
      Set<Integer> currentRange = PerfEnforceUtils.getRangeSet(currentSize);

      // get the worker diff
      Set<Integer> diff = com.google.common.collect.Sets.difference(previousRange, currentRange);

      // get the new relation key
      RelationKey currentRelationKeyToUnion =
          new RelationKey(relationKey.getUserName(), relationKey.getProgramName(), relationKey.getRelationName()
              + currentSize + "_U");

      // shuffle the diffs (from previous relation key) to the rest
      final ExchangePairID shuffleId = ExchangePairID.newID();
      DbQueryScan scan = new DbQueryScan(previousRelationKey, schema);

      int[] producingWorkers = PerfEnforceUtils.getRangeInclusiveArray(Collections.min(diff), Collections.max(diff));
      int[] receivingWorkers = PerfEnforceUtils.getRangeInclusiveArray(1, Collections.max(currentRange));

      GenericShuffleProducer producer =
          new GenericShuffleProducer(scan, shuffleId, receivingWorkers, new RoundRobinPartitionFunction(
              receivingWorkers.length));
      GenericShuffleConsumer consumer = new GenericShuffleConsumer(schema, shuffleId, producingWorkers);
      DbInsert insert = new DbInsert(consumer, currentRelationKeyToUnion, true);

      Map<Integer, RootOperator[]> workerPlans = new HashMap<>(currentSize);
      for (Integer workerID : producingWorkers) {
        workerPlans.put(workerID, new RootOperator[] { producer });
      }
      for (Integer workerID : receivingWorkers) {
        workerPlans.put(workerID, new RootOperator[] { insert });
      }
      try {
        server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlans).get();
        relationKeysToUnion.add(currentRelationKeyToUnion);

        RelationKey currentConfigRelationKey =
            new RelationKey(relationKey.getUserName(), relationKey.getProgramName(), relationKey.getRelationName()
                + currentSize);
        server.createView(currentConfigRelationKey.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL), PerfEnforceUtils
            .createUnionQuery(relationKeysToUnion), currentRange);
        server.addDatasetToCatalog(currentConfigRelationKey, schema, currentRange);
      } catch (InterruptedException | ExecutionException | DbException | CatalogException e) {
        e.printStackTrace();
      }

      previousRange = currentRange;
      previousRelationKey = currentRelationKeyToUnion;
    }
  }

  /*
   * Ingesting dimension tables for broadcasting
   */
  public void ingestDimension(final RelationKey relationKey, final DataSource source, final Schema schema,
      final Character delimiter, final Set<Integer> configurations) {

    Set<Integer> totalWorkers = PerfEnforceUtils.getRangeSet(Collections.max(configurations));

    try {
      server.ingestCSVDatasetInParallel(relationKey, source, schema, delimiter, totalWorkers);
    } catch (DbException | InterruptedException e1) {
      e1.printStackTrace();
    }

    DbQueryScan dbscan = new DbQueryScan(relationKey, schema);
    final ExchangePairID broadcastID = ExchangePairID.newID();

    int[][] cellPartition = new int[1][];
    int[] allCells = new int[totalWorkers.size()];
    for (int i = 0; i < totalWorkers.size(); i++) {
      allCells[i] = i;
    }
    cellPartition[0] = allCells;
    GenericShuffleProducer producer =
        new GenericShuffleProducer(dbscan, broadcastID, cellPartition, MyriaUtils.integerSetToIntArray(totalWorkers),
            new FixValuePartitionFunction(0));

    GenericShuffleConsumer consumer =
        new GenericShuffleConsumer(schema, broadcastID, MyriaUtils.integerSetToIntArray(totalWorkers));
    DbInsert insert = new DbInsert(consumer, relationKey, true);
    Map<Integer, RootOperator[]> workerPlans = new HashMap<>(totalWorkers.size());
    for (Integer workerID : totalWorkers) {
      workerPlans.put(workerID, new RootOperator[] { producer, insert });
    }

    try {
      server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlans).get();
    } catch (InterruptedException | ExecutionException | DbException | CatalogException e) {
      e.printStackTrace();
    }
  }

  /*
   * Run Statistics on the table by extending statistics space for each column and running analyze on the table on
   * worker #1
   */
  public void runPostgresStatistics(final TableDescriptionEncoding t) {
    for (int i = 0; i < t.schema.getColumnNames().size(); i++) {
      server.executeSQLCommand(String.format("ALTER TABLE %s ALTER COLUMN %s SET STATISTICS 500;", t.relationKey
          .toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL), t.schema.getColumnName(i)), new HashSet<Integer>(Arrays
          .asList(1)));
    }
    server.executeSQLCommand(String.format("ANALYZE %s;", t.relationKey
        .toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)), new HashSet<Integer>(Arrays.asList(1)));
  }

  /*
   * For each primary, determine the rank
   */
  public void runTableRanking(final double selectivity) {

  }

  public void generatePostgresFeatures(final String queryFilePath) {
    // run something on postgres and output results to some directly -- possibly the same as the configuration ---
    // this should first scan all
  }
}
