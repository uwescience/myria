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

  public PerfEnforceDriver(final Server server, final String instancePath) {
    configurationPath = (Paths.get(instancePath, "perfenforce_files"));
    this.server = server;
    dataPreparationStatus="";
    isDonePSLA = false;
  }

  /*
   * Fetch necessary files from S3
   */
  public void fetchS3Files() throws PerfEnforceException, Exception {
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

  public void preparePSLA(List<PerfEnforceTableEncoding> tableList) throws Exception {
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
        new PrintWriter(configurationPath.resolve("PSLAGeneration")
        								.resolve("SchemaDefinition.json").toString());
    out.print(schemaDefinitionFile);
    out.close();

    dataPreparationStatus = "Generating Queries for PSLA";
    PSLAManagerWrapper pslaManager = new PSLAManagerWrapper();
    pslaManager.generateQueries();
    perfenforceDataPrepare.collectFeaturesFromGeneratedQueries();

    pslaManager.generatePSLA();
    isDonePSLA = true;
  }
  public String getDataPreparationStatus()
  {
	  return dataPreparationStatus;
  }
  public boolean isDonePSLA() {
    return isDonePSLA;
  }

  public String getPSLA() throws PerfEnforceException {
    StringWriter output = new StringWriter();
    try {
      Reader input = new FileReader(new File(configurationPath.resolve("PSLAGeneration")
    		  												  .resolve("FinalPSLA.json").toString()));
      IOUtils.copy(input, output);
    } catch (IOException e) {
     throw new PerfEnforceException();
    }
    return output.toString();
  }

  public void setTier(final int tier) {
    perfenforceOnlineLearning = new PerfEnforceOnlineLearning(server, tier);
  }

  public void findSLA(String querySQL) throws PerfEnforceException{
    perfenforceOnlineLearning.findSLA(querySQL);
    perfenforceOnlineLearning.findBestClusterSize();
  }

  public void recordRealRuntime(final Double queryRuntime) throws PerfEnforceException {
    perfenforceOnlineLearning.recordRealRuntime(queryRuntime);
  }

  public PerfEnforceQueryMetadataEncoding getCurrentQuery() {
    return perfenforceOnlineLearning.getCurrentQuery();
  }

  public int getClusterSize() {
    return perfenforceOnlineLearning.getClusterSize();
  }
}
