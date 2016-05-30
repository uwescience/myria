/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.parallel.Server;

/**
 * The PerfEnforce Driver
 * 
 */
public class PerfEnforceDriver {

  String configFilePath;
  Set<Integer> configurations;
  HashMap<Integer, RelationKey> factTableMapper;
  TableDescriptionEncoding factTableDesc;

  // holds an instance of the PSLAManagerWrapper
  PSLAManagerWrapper pslaManager;

  public PerfEnforceDriver(final String configFilePath) {
    this.configFilePath = configFilePath;
    configurations = new HashSet<Integer>(Arrays.asList(4, 6, 8, 10, 12));
    factTableMapper = new HashMap<Integer, RelationKey>();

  }

  public void beginDataPreparation(final Server server) throws DbException, IOException {

    PerfEnforceDataPreparation dataPrepare = new PerfEnforceDataPreparation(server);
    List<TableDescriptionEncoding> allTables =
        PerfEnforceConfigurationParser.getAllTables(configFilePath + "deployment_tables.json");

    List<TableDescriptionEncoding> dimensionTables =
        PerfEnforceConfigurationParser.getTablesOfType("dimension", configFilePath + "deployment_tables.json");

    // ingest all relations
    for (TableDescriptionEncoding currentTable : allTables) {
      if (currentTable.type.equalsIgnoreCase("fact")) {
        if (factTableMapper.isEmpty()) {
          factTableMapper = dataPrepare.ingestFact(configurations, currentTable);
          factTableDesc = currentTable;
        }
      } else {
        if (server.getDatasetStatus(currentTable.relationKey) == null) {
          dataPrepare.ingestDimension(configurations, currentTable);
        }
      }
    }

    // run statistics on all columns of the fact partitions (UNIONS ONLY!!!)
    for (Entry<Integer, RelationKey> entry : factTableMapper.entrySet()) {
      // This is a yucky workaround to get the partition info (borrowed from the original table)
      TableDescriptionEncoding temp = factTableDesc;
      temp.relationKey =
          new RelationKey(entry.getValue().getUserName(), entry.getValue().getProgramName(), entry.getValue()
              .getRelationName()
              + "_U");
      dataPrepare.runPostgresStatistics(temp);
    }

    // run statistics on all columns of the dimension tables
    for (TableDescriptionEncoding d : dimensionTables) {
      dataPrepare.runPostgresStatistics(d);
    }

    // prepare query generation directories
    for (Integer config : configurations) {
      Path path = Paths.get(configFilePath + config + "_Workers/");
      try {
        Files.createDirectories(path);
      } catch (IOException e) {
        e.printStackTrace();
      }

      // We can make this better, but let's make sure it works for now
      ArrayList<StatsTableEncoding> statsTable = new ArrayList<StatsTableEncoding>();
      PrintWriter writer = new PrintWriter(path + "/stats.json", "UTF-8");
      // corresponding fact partition
      RelationKey factRelationKey = factTableMapper.get(config);
      long factTableCount = dataPrepare.runTableCount(factRelationKey, config);
      StatsTableEncoding factStats =
          dataPrepare.runTableRanking(factRelationKey, factTableCount, factTableDesc.keys, factTableDesc.schema);
      statsTable.add(factStats);

      for (TableDescriptionEncoding dimensionTableDesc : dimensionTables) {
        RelationKey dimensionTableKey = dimensionTableDesc.relationKey;
        long dimensionTableCount = dataPrepare.runTableCount(dimensionTableKey, 1);
        StatsTableEncoding dimensionStats =
            dataPrepare.runTableRanking(dimensionTableKey, dimensionTableCount, dimensionTableDesc.keys,
                dimensionTableDesc.schema);
        statsTable.add(dimensionStats);
      }
      ObjectMapper mapper = new ObjectMapper();
      try {
        mapper.writeValue(writer, statsTable);
      } catch (IOException e) {
        e.printStackTrace();
      }
      writer.close();

      // pslaManager.generateQueries(path);

      // generate features for THIS config
      // dataPrepare.generatePostgresFeatures(path);
    }
    // generate PSLA for all configs given all the features
    pslaManager.generatePSLA();

  }

  public void beginQueryMonitoring() {
    // for each incoming query must ask for subsumption
  }

}
