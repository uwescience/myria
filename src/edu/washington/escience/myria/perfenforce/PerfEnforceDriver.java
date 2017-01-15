/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.gson.Gson;

import edu.washington.escience.myria.api.encoding.PerfEnforceQueryMetadataEncoding;
import edu.washington.escience.myria.api.encoding.PerfEnforceTableEncoding;
import edu.washington.escience.myria.parallel.Server;

/**
 * The PerfEnforce Driver
 *
 */
public final class PerfEnforceDriver {

  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PerfEnforceDriver.class);

  final public static List<Integer> configurations = Arrays.asList(4, 6, 8, 10, 12);

  public static Path configurationPath;

  public static List<PerfEnforceTableEncoding> tableList;
  public static PerfEnforceTableEncoding factTableDesc;

  private final Server server;
  private String dataPreparationStatus;
  private boolean isDonePSLA;

  private PerfEnforceOnlineLearning perfenforceOnlineLearning;

  /**
   * Constructor for the PerfEnforceDriver
   * 
   * @param server the instance of the server
   * @param instancePath the path for the Myria deployment
   */
  public PerfEnforceDriver(final Server server, final String instancePath) {
    configurationPath = (Paths.get(instancePath, "perfenforce_files"));
    this.server = server;
    dataPreparationStatus = "";
    isDonePSLA = false;
  }

  /**
   * Fetch necessary files from S3
   */
  public void fetchS3Files() throws PerfEnforceException {
    AmazonS3 s3Client = new AmazonS3Client(new AnonymousAWSCredentials());
    String currentFile = "";
    BufferedReader bufferedReader;
    try {
      bufferedReader =
          new BufferedReader(
              new FileReader(configurationPath.resolve("filesToFetch.txt").toString()));
      while ((currentFile = bufferedReader.readLine()) != null) {
        Path filePath = configurationPath.resolve(currentFile);
        s3Client.getObject(
            new GetObjectRequest("perfenforce", currentFile), new File(filePath.toString()));
      }
      bufferedReader.close();
    } catch (Exception e) {
      throw new PerfEnforceException("Error while fetching files from S3");
    }
  }

  /**
   * Prepares the PSLA given a list of tables
   * 
   * @param tableList the tables from the user's dataset
   */
  public void preparePSLA(List<PerfEnforceTableEncoding> tableList) throws Exception {
    isDonePSLA = false;
    PerfEnforceDriver.tableList = tableList;
    fetchS3Files();

    PerfEnforceDataPreparation perfenforceDataPrepare = new PerfEnforceDataPreparation(server);

    dataPreparationStatus = "Ingesting Data";
    for (PerfEnforceTableEncoding currentTable : tableList) {
      if (currentTable.type.equalsIgnoreCase("fact")) {
        perfenforceDataPrepare.ingestFact(currentTable);
        factTableDesc = currentTable;
      } else {
        perfenforceDataPrepare.ingestDimension(currentTable);
      }
      dataPreparationStatus = "Analyzing Data";
      perfenforceDataPrepare.analyzeTable(currentTable);
    }

    dataPreparationStatus = "Collecting Statistics";
    perfenforceDataPrepare.collectSelectivities();

    Gson gson = new Gson();
    String schemaDefinitionFile = gson.toJson(tableList);
    PrintWriter out =
        new PrintWriter(
            configurationPath
                .resolve("PSLAGeneration")
                .resolve("SchemaDefinition.json")
                .toString());
    out.print(schemaDefinitionFile);
    out.close();

    dataPreparationStatus = "Generating Queries for PSLA";
    PSLAManagerWrapper pslaManager = new PSLAManagerWrapper();
    pslaManager.generateQueries();
    perfenforceDataPrepare.collectFeaturesFromGeneratedQueries();
    pslaManager.generatePSLA();

    dataPreparationStatus = "Finished";
    isDonePSLA = true;
  }

  /**
   * Returns the status of the PSLA generation process
   */
  public String getDataPreparationStatus() {
    return dataPreparationStatus;
  }

  /**
   * Return a boolean to determine whether the PSLA is finished generating
   */
  public boolean isDonePSLA() {
    return isDonePSLA;
  }

  /**
   * Returns the final PSLA
   */
  public String getPSLA() throws PerfEnforceException {
    StringWriter output = new StringWriter();
    try {
      Reader input =
          new FileReader(
              new File(
                  configurationPath
                      .resolve("PSLAGeneration")
                      .resolve("FinalPSLA.json")
                      .toString()));
      IOUtils.copy(input, output);
    } catch (IOException e) {
      throw new PerfEnforceException();
    }
    return output.toString();
  }

  /**
   * Sets the tier selected by the user
   * 
   * @param tier the tier selected
   */
  public void setTier(final int tier) {
    perfenforceOnlineLearning = new PerfEnforceOnlineLearning(server, tier);
  }

  /**
   * Finds the SLA for a given query
   * 
   * @param querySQL the given query
   */
  public void findSLA(String querySQL) throws PerfEnforceException {
    perfenforceOnlineLearning.findSLA(querySQL);
    perfenforceOnlineLearning.findBestConfigurationSize();
  }

  /**
   * Records the runtime of a query
   * 
   * @param queryRuntime the runtime of a query
   */
  public void recordRealRuntime(final Double queryRuntime) throws PerfEnforceException {
    perfenforceOnlineLearning.recordRealRuntime(queryRuntime);
  }

  /**
   * Returns information about the current query, assuming the user recently requested the SLA
   */
  public PerfEnforceQueryMetadataEncoding getCurrentQuery() {
    return perfenforceOnlineLearning.getCurrentQuery();
  }

  /**
   * Returns the current cluster size
   */
  public int getClusterSize() {
    return perfenforceOnlineLearning.getClusterSize();
  }
}
