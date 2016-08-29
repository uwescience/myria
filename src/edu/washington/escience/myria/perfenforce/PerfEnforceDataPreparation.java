/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.PerfEnforceStatisticsEncoding;
import edu.washington.escience.myria.api.encoding.PerfEnforceTableEncoding;
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

  private HashMap<Integer, RelationKey> factTableRelationMapper;
  private PerfEnforceTableEncoding factTableDescription;

  /** Logger. */
  protected static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(PerfEnforceDataPreparation.class);

  public PerfEnforceDataPreparation(final Server server) {
    this.server = server;
  }

  /*
   * Ingesting the fact table in a parallel sequence
   */
  public HashMap<Integer, RelationKey> ingestFact(final PerfEnforceTableEncoding factTableDesc)
      throws Exception {
    factTableDescription = factTableDesc;
    factTableRelationMapper = new HashMap<Integer, RelationKey>();

    ArrayList<RelationKey> relationKeysToUnion = new ArrayList<RelationKey>();
    Collections.sort(PerfEnforceDriver.configurations, Collections.reverseOrder());

    // Create a sequence for the largest cluster size
    int maxConfig = PerfEnforceDriver.configurations.get(0);
    Set<Integer> maxWorkerRange = PerfEnforceUtils.getWorkerRangeSet(maxConfig);

    /*
     * First, ingest the fact table under the relationKey with the union ("_U"). Then, create a materialized view with
     * the original relationKey name and add it to the catalog. This is what the user will be using on the MyriaL front
     * end.
     */
    try {
      RelationKey relationKeyWithUnion =
          new RelationKey(
              factTableDesc.relationKey.getUserName(),
              factTableDesc.relationKey.getProgramName(),
              factTableDesc.relationKey.getRelationName() + maxConfig + "_U");

      server.parallelIngestDataset(
          relationKeyWithUnion,
          factTableDesc.schema,
          factTableDesc.delimiter,
          null,
          null,
          null,
          factTableDesc.source,
          maxWorkerRange,
          null);

      relationKeysToUnion.add(relationKeyWithUnion);

      RelationKey relationKeyOriginal =
          new RelationKey(
              factTableDesc.relationKey.getUserName(),
              factTableDesc.relationKey.getProgramName(),
              factTableDesc.relationKey.getRelationName() + maxConfig);
      server.createMaterializedView(
          relationKeyOriginal.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL),
          PerfEnforceUtils.createUnionQuery(relationKeysToUnion),
          maxWorkerRange);
      server.addDatasetToCatalog(relationKeyOriginal, factTableDesc.schema, maxWorkerRange);
      factTableRelationMapper.put(maxConfig, relationKeyOriginal);

      /*
       * Iterate and run this for the rest of the workers
       */
      Set<Integer> previousWorkerRange = maxWorkerRange;
      RelationKey previousRelationKey = relationKeyWithUnion;
      for (int c = 1; c < PerfEnforceDriver.configurations.size(); c++) {
        // Get the next sequence of workers
        int currentSize = PerfEnforceDriver.configurations.get(c);
        Set<Integer> currentWorkerRange = PerfEnforceUtils.getWorkerRangeSet(currentSize);
        Set<Integer> diff = Sets.difference(previousWorkerRange, currentWorkerRange);

        RelationKey currentRelationKeyToUnion =
            new RelationKey(
                factTableDesc.relationKey.getUserName(),
                factTableDesc.relationKey.getProgramName(),
                factTableDesc.relationKey.getRelationName() + currentSize + "_U");

        final ExchangePairID shuffleId = ExchangePairID.newID();
        DbQueryScan scan = new DbQueryScan(previousRelationKey, factTableDesc.schema);

        int[] producingWorkers =
            PerfEnforceUtils.getRangeInclusiveArray(Collections.min(diff), Collections.max(diff));
        int[] receivingWorkers =
            PerfEnforceUtils.getRangeInclusiveArray(1, Collections.max(currentWorkerRange));

        GenericShuffleProducer producer =
            new GenericShuffleProducer(
                scan,
                shuffleId,
                receivingWorkers,
                new RoundRobinPartitionFunction(receivingWorkers.length));
        GenericShuffleConsumer consumer =
            new GenericShuffleConsumer(factTableDesc.schema, shuffleId, producingWorkers);
        DbInsert insert = new DbInsert(consumer, currentRelationKeyToUnion, true);

        Map<Integer, RootOperator[]> workerPlans = new HashMap<>(currentSize);
        for (Integer workerID : producingWorkers) {
          workerPlans.put(workerID, new RootOperator[] {producer});
        }
        for (Integer workerID : receivingWorkers) {
          workerPlans.put(workerID, new RootOperator[] {insert});
        }

        server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlans).get();
        relationKeysToUnion.add(currentRelationKeyToUnion);

        RelationKey currentConfigRelationKey =
            new RelationKey(
                factTableDesc.relationKey.getUserName(),
                factTableDesc.relationKey.getProgramName(),
                factTableDesc.relationKey.getRelationName() + currentSize);
        server.createMaterializedView(
            currentConfigRelationKey.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL),
            PerfEnforceUtils.createUnionQuery(relationKeysToUnion),
            currentWorkerRange);
        server.addDatasetToCatalog(
            currentConfigRelationKey, factTableDesc.schema, currentWorkerRange);
        factTableRelationMapper.put(currentSize, currentConfigRelationKey);
        previousWorkerRange = currentWorkerRange;
        previousRelationKey = currentConfigRelationKey;
      }
      return factTableRelationMapper;
    } catch (Exception e) {
      throw e;
      //throw new PerfEnforceException("Error while ingesting fact table");
    }
  }

  /*
   * Ingesting dimension tables for broadcasting
   */
  public void ingestDimension(final PerfEnforceTableEncoding dimTableDesc) throws Exception {

    Set<Integer> totalWorkers =
        PerfEnforceUtils.getWorkerRangeSet(Collections.max(PerfEnforceDriver.configurations));

    try {
      server.parallelIngestDataset(
          dimTableDesc.relationKey,
          dimTableDesc.schema,
          dimTableDesc.delimiter,
          null,
          null,
          null,
          dimTableDesc.source,
          totalWorkers,
          null);

      DbQueryScan dbscan = new DbQueryScan(dimTableDesc.relationKey, dimTableDesc.schema);
      /*
       * Is there a better way to broadcast relations?
       */
      final ExchangePairID broadcastID = ExchangePairID.newID();
      int[][] cellPartition = new int[1][];
      int[] allCells = new int[totalWorkers.size()];
      for (int i = 0; i < totalWorkers.size(); i++) {
        allCells[i] = i;
      }
      cellPartition[0] = allCells;

      GenericShuffleProducer producer =
          new GenericShuffleProducer(
              dbscan,
              broadcastID,
              cellPartition,
              MyriaUtils.integerSetToIntArray(totalWorkers),
              new FixValuePartitionFunction(0));

      GenericShuffleConsumer consumer =
          new GenericShuffleConsumer(
              dimTableDesc.schema, broadcastID, MyriaUtils.integerSetToIntArray(totalWorkers));
      DbInsert insert = new DbInsert(consumer, dimTableDesc.relationKey, true);
      Map<Integer, RootOperator[]> workerPlans = new HashMap<>(totalWorkers.size());
      for (Integer workerID : totalWorkers) {
        workerPlans.put(workerID, new RootOperator[] {producer, insert});
      }

      server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlans).get();
    } catch (Exception e) {
      throw e;
      //throw new PerfEnforceException("Error ingesting dimension tables");
    }
  }

  /*
   * Run Statistics on the table by extending statistics space for each column and running analyze on the table for all
   * workers
   */
  public void analyzeTable(final PerfEnforceTableEncoding t)
      throws DbException, InterruptedException {
    /*
     * If this table is Fact, we need to make sure we run "analyze" on all versions of the table
     */
    if (t.type.equalsIgnoreCase("fact")) {
      for (Entry<Integer, RelationKey> entry : factTableRelationMapper.entrySet()) {
        PerfEnforceTableEncoding temp =
            new PerfEnforceTableEncoding(
                t.relationKey,
                t.type,
                t.source,
                t.schema,
                t.delimiter,
                t.keys,
                t.corresponding_fact_key);
        temp.relationKey =
            new RelationKey(
                entry.getValue().getUserName(),
                entry.getValue().getProgramName(),
                entry.getValue().getRelationName());
        postgresStatsAnalyzeTable(temp, PerfEnforceUtils.getWorkerRangeSet(entry.getKey()));
      }
    } else {
      postgresStatsAnalyzeTable(
          t, PerfEnforceUtils.getWorkerRangeSet(Collections.max(PerfEnforceDriver.configurations)));
    }
  }

  public void postgresStatsAnalyzeTable(final PerfEnforceTableEncoding t, Set<Integer> workers)
      throws DbException, InterruptedException {
    for (int i = 0; i < t.schema.getColumnNames().size(); i++) {
      server.executeSQLStatement(
          String.format(
              "ALTER TABLE %s ALTER COLUMN %s SET STATISTICS 500;",
              t.relationKey.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL),
              t.schema.getColumnName(i)),
          workers);
    }
    server.executeSQLStatement(
        String.format(
            "ANALYZE %s;", t.relationKey.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)),
        workers);
  }

  public void collectSelectivities() throws PerfEnforceException, Exception {

    try {
      /* record the stats for each configuration */
      for (Integer currentConfig : PerfEnforceDriver.configurations) {

        Path statsWorkerPath =
            PerfEnforceDriver.configurationPath
                .resolve(currentConfig + "_Workers")
                .resolve("stats.json");
        List<PerfEnforceStatisticsEncoding> statsEncodingList =
            new ArrayList<PerfEnforceStatisticsEncoding>();

        RelationKey factRelationKey = factTableRelationMapper.get(currentConfig);
        long factTableTupleCount = server.getDatasetStatus(factRelationKey).getNumTuples();
        statsEncodingList.add(
            runTableRanking(
                factRelationKey,
                factTableTupleCount,
                currentConfig,
                factTableDescription.type,
                factTableDescription.keys,
                factTableDescription.schema));

        for (PerfEnforceTableEncoding t : PerfEnforceDriver.tableList) {
          if (t.type.equalsIgnoreCase("dimension")) {
            RelationKey dimensionTableKey = t.relationKey;
            long dimensionTableTupleCount =
                server.getDatasetStatus(dimensionTableKey).getNumTuples();
            statsEncodingList.add(
                runTableRanking(
                    dimensionTableKey,
                    dimensionTableTupleCount,
                    Collections.max(PerfEnforceDriver.configurations),
                    t.type,
                    t.keys,
                    t.schema));
          }
        }

        PrintWriter statsObjectWriter =
            new PrintWriter(new FileOutputStream(new File(statsWorkerPath.toString())));
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(statsObjectWriter, statsEncodingList);
        statsObjectWriter.close();
      }
    } catch (Exception e) {
      throw e;
      //throw new PerfEnforceException("Error collecting table statistics");
    }
  }

  /*
   * For each primary key, determine the rank based on the selectivity and return the result
   */
  public PerfEnforceStatisticsEncoding runTableRanking(
      final RelationKey relationKey,
      final long tableSize,
      final int config,
      final String type,
      final Set<Integer> keys,
      final Schema schema)
      throws PerfEnforceException {

    List<String> selectivityKeys = new ArrayList<String>();
    List<Double> selectivityList = Arrays.asList(new Double[] {.001, .01, .1});

    String attributeKeyString = PerfEnforceUtils.getAttributeKeyString(keys, schema);
    Schema attributeKeySchema = PerfEnforceUtils.getAttributeKeySchema(keys, schema);

    String tableName = relationKey.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL);
    try {
      for (int i = 0; i < selectivityList.size(); i++) {
        String rankingQuery =
            String.format(
                "select %s from (select %s, CAST(rank() over (order by %s asc) AS float)/%s as rank from %s) as r where r.rank >= %s LIMIT 1;",
                attributeKeyString,
                attributeKeyString,
                attributeKeyString,
                tableSize / config,
                tableName,
                selectivityList.get(i));
        String[] sqlResult =
            server.executeSQLStatement(
                rankingQuery, attributeKeySchema, new HashSet<Integer>(Arrays.asList(1)));

        selectivityKeys.add(sqlResult[0]);
      }

      // HACK: We can't properly "count" tuples for tables that are broadcast
      long modifiedSize = tableSize;
      if (type.equalsIgnoreCase("dimension")) {
        modifiedSize = tableSize / Collections.max(PerfEnforceDriver.configurations);
      }

      return new PerfEnforceStatisticsEncoding(tableName, modifiedSize, selectivityKeys);
    } catch (Exception e) {
      throw new PerfEnforceException("error running table ranks");
    }
  }

  public void collectFeaturesFromGeneratedQueries() throws PerfEnforceException {

    for (Integer config : PerfEnforceDriver.configurations) {
      Path workerPath =
          Paths.get(PerfEnforceDriver.configurationPath.toString(), config + "_Workers");
      String currentLine = "";

      try {
        PrintWriter featureWriter =
            new PrintWriter(workerPath.resolve("TESTING.arff").toString(), "UTF-8");

        featureWriter.write("@relation testing \n");
        featureWriter.write("@attribute numberTables numeric \n");
        featureWriter.write("@attribute postgesEstCostMin numeric \n");
        featureWriter.write("@attribute postgesEstCostMax numeric \n");
        featureWriter.write("@attribute postgesEstNumRows numeric \n");
        featureWriter.write("@attribute postgesEstWidth numeric \n");
        featureWriter.write("@attribute numberOfWorkers numeric \n");
        featureWriter.write("@attribute realTime numeric \n");
        featureWriter.write("\n");
        featureWriter.write("@data \n");

        BufferedReader br =
            new BufferedReader(
                new FileReader(workerPath.resolve("SQLQueries-Generated.txt").toString()));
        while ((currentLine = br.readLine()) != null) {
          currentLine =
              currentLine.replace(
                  factTableDescription.relationKey.getRelationName(),
                  factTableRelationMapper.get(config).getRelationName());
          String explainQuery = "EXPLAIN " + currentLine;
          String features = PerfEnforceUtils.getMaxFeature(server, explainQuery, config);
          featureWriter.write(features + "\n");
        }
        featureWriter.close();
        br.close();
      } catch (Exception e) {
        throw new PerfEnforceException("Error creating table features");
      }
    }
  }
}
