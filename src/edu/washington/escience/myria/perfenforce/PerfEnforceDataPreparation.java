/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
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

  /** Logger. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PerfEnforceDataPreparation.class);

  public PerfEnforceDataPreparation(final Server server) {
    this.server = server;
  }

  /*
   * Ingesting the fact table in a parallel sequence
   */
  public HashMap<Integer, RelationKey> ingestFact(final Set<Integer> configurations,
      final TableDescriptionEncoding tableDesc) {
    // Mapper to Return
    HashMap<Integer, RelationKey> factTableMapper = new HashMap<Integer, RelationKey>();

    // Table Parameters
    RelationKey relationKey = tableDesc.relationKey;
    DataSource source = tableDesc.source;
    Schema schema = tableDesc.schema;
    Character delimiter = tableDesc.delimiter;

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
      factTableMapper.put(maxConfig, maxConfigRelationKey);
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
        factTableMapper.put(currentSize, currentConfigRelationKey);
      } catch (InterruptedException | ExecutionException | DbException | CatalogException e) {
        e.printStackTrace();
      }

      previousRange = currentRange;
      previousRelationKey = currentRelationKeyToUnion;
    }
    return factTableMapper;
  }

  /*
   * Ingesting dimension tables for broadcasting
   */
  public void ingestDimension(final Set<Integer> configurations, final TableDescriptionEncoding tableDesc) {
    // Table Parameters
    RelationKey relationKey = tableDesc.relationKey;
    DataSource source = tableDesc.source;
    Schema schema = tableDesc.schema;
    Character delimiter = tableDesc.delimiter;

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
   * For each primary key, determine the rank based on the selectivity and return the result
   */
  public StatsTableEncoding runTableRanking(final RelationKey relationKey, final long tableSize,
      final Set<Integer> keys, final Schema schema) throws IOException, DbException {
    String keyString = "";

    // handle the more than 1 key scenario (just in case)
    int counter = 1;
    for (int key : keys) {
      keyString += schema.getColumnName(key);
      if (counter != keys.size()) {
        keyString += ",";
      }
      counter++;
    }

    // for each selectivity
    List<Double> selectivityList = Arrays.asList(new Double[] { .001, .01, .1, 1.0 });

    // find the table stats
    StatsTableEncoding tableStats = null;

    String tableName = relationKey.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL);
    List<String> selectivityKeys = new ArrayList<String>();
    for (int i = 0; i < selectivityList.size(); i++) {
      String rankingQuery =
          String
              .format(
                  "select %s from (select %s, CAST(rank() over (order by %s asc) AS float)/%s as rank from %s) as r where r.rank >= %s LIMIT 1;",
                  keyString, keyString, keyString, tableSize, relationKey
                      .toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL), selectivityList.get(i));
      // only run on worker 1
      String result =
          server.executeSQLCommandSingleRowSingleWorker(rankingQuery, Schema.ofFields("count", Type.STRING_TYPE), 1);
      selectivityKeys.add(result);
    }

    tableStats =
        new StatsTableEncoding(tableName, tableSize, selectivityKeys.get(0), selectivityKeys.get(1), selectivityKeys
            .get(2));

    return tableStats;
  }

  /*
   * Get the table count
   */
  public long runTableCount(final RelationKey relationKey, final int config) throws DbException {
    return server.getDatasetStatus(relationKey).getNumTuples() / config;

  }

  public void generatePostgresFeatures(final Path path) {
    // run something on postgres and output results to some directly -- possibly the same as the configuration ---
    // this should first scan all

    /*
     * String outputCall = String .format(
     * "\\o | head -3 %s | tail -1 | sed  -e 's/.*cost=//' -e 's/\\.\\./,/' -e 's/ rows=/,/' -e 's/ width=/,/' -e 's/)//' | cat - >> %s;"
     * , "", "");
     * 
     * try { BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path + "queries.txt")));
     * // run the query on worker send output to file(replace file) // read the file first line, parse the output (sed?)
     * and put in output file } catch (FileNotFoundException e) { e.printStackTrace(); }
     */

  }
}
