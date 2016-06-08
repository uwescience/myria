/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.perfenforce.encoding.InitializeScalingEncoding;
import edu.washington.escience.myria.perfenforce.encoding.ScalingAlgorithmEncoding;
import edu.washington.escience.myria.perfenforce.encoding.StatsTableEncoding;
import edu.washington.escience.myria.perfenforce.encoding.TableDescriptionEncoding;

/**
 * The PerfEnforce Driver
 * 
 */
public class PerfEnforceDriver {

  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PerfEnforceDriver.class);

  static Set<Integer> configurations;
  HashMap<Integer, RelationKey> factTableMapper;
  TableDescriptionEncoding factTableDesc;

  // holds an instance of the PSLAManagerWrapper
  PSLAManagerWrapper pslaManager;
  public PerfEnforceDataPreparation dataPrepare;
  public PerfEnforceScalingAlgorithms perfenforceScaling;

  public PerfEnforceDriver() {
    configurations = new HashSet<Integer>(Arrays.asList(4, 6, 8, 10, 12));
    factTableMapper = new HashMap<Integer, RelationKey>();
    pslaManager = new PSLAManagerWrapper();
  }

  /*
   * NOTE: move more of this logic to the data preparation class
   */
  public void beginDataPreparation(final Server server, final String configFilePath) throws DbException, IOException {
    pslaManager.fetchS3Files(configFilePath);

    dataPrepare = new PerfEnforceDataPreparation(server);
    List<TableDescriptionEncoding> allTables =
        PerfEnforceConfigurationParser.getAllTables(configFilePath + "SchemaDefinition.json");

    List<TableDescriptionEncoding> dimensionTables =
        PerfEnforceConfigurationParser.getTablesOfType("dimension", configFilePath + "SchemaDefinition.json");

    // ingest all relations
    for (TableDescriptionEncoding currentTable : allTables) {
      if (currentTable.type.equalsIgnoreCase("fact")) {
        factTableDesc = currentTable;
        factTableMapper = dataPrepare.ingestFact(configurations, currentTable);

      } else {
        if (server.getDatasetStatus(currentTable.relationKey) == null) {
          dataPrepare.ingestDimension(configurations, currentTable);
        }
      }
    }

    // run statistics on all columns of the fact partitions (UNIONS ONLY!!!)
    for (Entry<Integer, RelationKey> entry : factTableMapper.entrySet()) {
      // This is a yucky workaround to get the partition info (borrowed from the original table)
      TableDescriptionEncoding temp =
          new TableDescriptionEncoding(factTableDesc.relationKey, factTableDesc.type, factTableDesc.source,
              factTableDesc.schema, factTableDesc.delimiter, factTableDesc.keys, factTableDesc.corresponding_fact_key);

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

    // prepare the stats.json on all directories
    for (Integer config : configurations) {
      Path path = Paths.get(configFilePath + config + "_Workers/");

      try {
        // This should be changed from the PSLAManager side. Shouldn't have to copy the file over
        String copyCmd = "cp " + configFilePath + "/SchemaDefinition.json " + configFilePath + config + "_Workers/";
        Runtime.getRuntime().exec(copyCmd);

        copyCmd = "cp " + configFilePath + "/tiers.txt " + configFilePath + config + "_Workers/";
        Runtime.getRuntime().exec(copyCmd);
      } catch (IOException e) {
        e.printStackTrace();
      }

      // We can make this better, but let's make sure it works for now
      ArrayList<StatsTableEncoding> statsTable = new ArrayList<StatsTableEncoding>();
      PrintWriter writer = new PrintWriter(path + "/stats.json", "UTF-8");
      // corresponding fact partition
      RelationKey factRelationKey = factTableMapper.get(config);
      long factTableCount = dataPrepare.runTableCount(factRelationKey);
      StatsTableEncoding factStats =
          dataPrepare.runTableRanking(factRelationKey, factTableCount, config, factTableDesc.type, factTableDesc.keys,
              factTableDesc.schema);
      statsTable.add(factStats);

      for (TableDescriptionEncoding dimensionTableDesc : dimensionTables) {
        RelationKey dimensionTableKey = dimensionTableDesc.relationKey;
        long dimensionTableCount = dataPrepare.runTableCount(dimensionTableKey);
        StatsTableEncoding dimensionStats =
            dataPrepare.runTableRanking(dimensionTableKey, dimensionTableCount, Collections.max(configurations),
                dimensionTableDesc.type, dimensionTableDesc.keys, dimensionTableDesc.schema);
        statsTable.add(dimensionStats);
      }
      ObjectMapper mapper = new ObjectMapper();
      try {
        mapper.writeValue(writer, statsTable);
      } catch (IOException e) {
        e.printStackTrace();
      }
      writer.close();
    }

    // generate queries
    pslaManager.generateQueries(configFilePath);

    for (Integer config : configurations) {
      // read the resulting queries
      String currentLine = "";
      PrintWriter featureWriter = new PrintWriter(configFilePath + config + "_Workers/" + "TESTING.arff", "UTF-8");
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
          new BufferedReader(new FileReader(configFilePath + config + "_Workers/" + "SQLQueries-Generated.txt"));
      while ((currentLine = br.readLine()) != null) {
        /* intercept the fact table name */
        currentLine =
            currentLine.replace(factTableDesc.relationKey.getRelationName(), factTableMapper.get(config)
                .getRelationName());
        String features = dataPrepare.generatePostgresFeatures(currentLine);
        features = features.substring(features.indexOf("cost"));
        features = features.replace("\"", " ");
        String[] cmd =
            {
                "sh",
                "-c",
                "echo \"" + features
                    + "\" | sed -e 's/.*cost=//' -e 's/\\.\\./,/' -e 's/ rows=/,/' -e 's/ width=/,/' -e 's/)//'" };
        ProcessBuilder pb = new ProcessBuilder(cmd);
        Process p = pb.start();

        BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
        features = input.readLine();

        // add the extra features -- hacky
        if (currentLine.contains("WHERE")) {
          String[] tables = currentLine.substring(currentLine.indexOf("FROM"), currentLine.indexOf("WHERE")).split(",");
          features = tables.length + "," + features;
        } else {
          features = "1," + features;
        }

        features += "," + config + ",0";

        featureWriter.write(features + "\n");
      }
      featureWriter.close();
      br.close();
    }

    // generate PSLA for all configs given all the features
    pslaManager.generatePSLA(configFilePath);

  }

  public void beginQueryMonitoring(final InitializeScalingEncoding scalingAlgorithm) {
    perfenforceScaling = new PerfEnforceScalingAlgorithms(scalingAlgorithm);
  }

  // Collect data from ith line in query-meta-data in the appropriate sequence
  public void postFakeQuery(final ScalingAlgorithmEncoding scalingAlgorithmEncoding) {

    perfenforceScaling.updateParameters(scalingAlgorithmEncoding);

    if (perfenforceScaling.scalingAlgorithm instanceof ReinforcementLearning
        || perfenforceScaling.scalingAlgorithm instanceof PIControl) {
      perfenforceScaling.setupNextFakeQuery();
      perfenforceScaling.step();
    } else {
      // Tentative for proactive
      perfenforceScaling.step();
      perfenforceScaling.setupNextFakeQuery();
    }

    perfenforceScaling.incrementQueryCounter();

  }

  // For real queries
  // Given a query metadata from the query interception
  // q should only be given an SLA and id...
  public void postQuery(final QueryMetaData q) {

  }

}
