/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.washington.escience.myria.parallel.Server;

/**
 * The PerfEnforce Driver
 * 
 */
public class PerfEnforceDriver {

  String configFilePath;
  Set<Integer> configurations;

  public PerfEnforceDriver(final String configFilePath) {
    this.configFilePath = configFilePath;
    configurations = new HashSet<Integer>(Arrays.asList(4, 6, 8, 10, 12));
  }

  public void beginDataPreparation(final Server server) {

    PerfEnforceDataPreparation dataPrepare = new PerfEnforceDataPreparation(server);

    // find all fact tables from the parsed config file
    List<TableDescriptionEncoding> factTables = PerfEnforceConfigurationParser.getTablesOfType("fact", configFilePath);
    for (int i = 0; i < factTables.size(); i++) {
      TableDescriptionEncoding currentTable = factTables.get(i);
      dataPrepare.ingestFact(currentTable.relationKey, currentTable.source, currentTable.schema,
          currentTable.delimiter, configurations);
    }

    // find all dimension tables from the parsed config file
    List<TableDescriptionEncoding> dimensionTables =
        PerfEnforceConfigurationParser.getTablesOfType("dimension", configFilePath);
    for (int i = 0; i < dimensionTables.size(); i++) {
      TableDescriptionEncoding currentTable = dimensionTables.get(i);
      dataPrepare.ingestDimension(currentTable.relationKey, currentTable.source, currentTable.schema,
          currentTable.delimiter, configurations);
    }

    // run statistics on all columns for all tables for worker #1
    for (TableDescriptionEncoding t : factTables) {
      dataPrepare.runPostgresStatistics(t);
    }
    for (TableDescriptionEncoding t : dimensionTables) {
      dataPrepare.runPostgresStatistics(t);
    }

    // Finally, call the PSLAWrapper
    PSLAManagerWrapper pslaManager = new PSLAManagerWrapper();
    String queryFilePath = pslaManager.generateQueries(configFilePath);
    dataPrepare.generatePostgresFeatures(queryFilePath);
    pslaManager.generatePSLA();
  }

  public void beginQueryMonitoring() {
  }

}
