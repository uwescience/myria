/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
  PSLAManagerWrapper pslaManager = new PSLAManagerWrapper();

  public PerfEnforceDriver(final String configFilePath) {
    this.configFilePath = configFilePath;
    configurations = new HashSet<Integer>(Arrays.asList(4, 6, 8, 10, 12));
    factTableMapper = new HashMap<Integer, RelationKey>();
  }

  public void beginDataPreparation(final Server server) throws FileNotFoundException, UnsupportedEncodingException,
      DbException {

    PerfEnforceDataPreparation dataPrepare = new PerfEnforceDataPreparation(server);
    List<TableDescriptionEncoding> allTables = PerfEnforceConfigurationParser.getAllTables(configFilePath);

    // ingest all relations
    for (TableDescriptionEncoding currentTable : allTables) {
      if (server.getDatasetStatus(currentTable.relationKey) != null) {
        if (currentTable.type.equalsIgnoreCase("fact")) {
          factTableMapper = dataPrepare.ingestFact(configurations, currentTable);
          factTableDesc = currentTable;
        } else {
          dataPrepare.ingestDimension(configurations, currentTable);
        }
      }
    }

    // run statistics on all columns for all tables for worker #1
    for (TableDescriptionEncoding t : allTables) {
      dataPrepare.runPostgresStatistics(t);
    }

    // prepare query generation directories
    for (Integer config : configurations) {
      Path path = Paths.get(configFilePath + config + "_Workers");
      try {
        Files.createDirectories(path);
      } catch (IOException e) {
        e.printStackTrace();
      }

      // We can make this better, but let's make sure it works for now
      ArrayList<StatsTableEncoding> statsTable = new ArrayList<StatsTableEncoding>();
      PrintWriter writer = new PrintWriter(path + "stats.json", "UTF-8");
      // corresponding fact partition
      RelationKey factRelationKey = factTableMapper.get(config);
      int factTableCount = dataPrepare.runTableCount(factRelationKey);
      StatsTableEncoding factStats =
          dataPrepare.runTableRanking(factRelationKey, factTableCount, factTableDesc.keys, factTableDesc.schema, path
              .toString());
      statsTable.add(factStats);

      for (TableDescriptionEncoding dimensionTableDesc : PerfEnforceConfigurationParser.getTablesOfType("dimension",
          configFilePath)) {
        RelationKey dimensionTableKey = dimensionTableDesc.relationKey;
        int dimensionTableCount = dataPrepare.runTableCount(dimensionTableKey);
        StatsTableEncoding dimensionStats =
            dataPrepare.runTableRanking(dimensionTableKey, dimensionTableCount, dimensionTableDesc.keys,
                dimensionTableDesc.schema, path.toString());
        statsTable.add(dimensionStats);
      }
      ObjectMapper mapper = new ObjectMapper();
      try {
        mapper.writeValue(writer, statsTable);
      } catch (IOException e) {
        e.printStackTrace();
      }
      writer.close();

      String queryFilePath = pslaManager.generateQueries(configFilePath);
      dataPrepare.generatePostgresFeatures(queryFilePath);

      // run a function to collect features for the configuration
    }
    // generate PSLA for all configs given all the features
    pslaManager.generatePSLA();

  }

  public void beginQueryMonitoring() {
    // for each incoming query must ask for subsumption
  }

}
