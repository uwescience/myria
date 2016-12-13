/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;

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

  private final Server server;
  private boolean isDonePSLA;

  private PerfEnforceOnlineLearning perfenforceOnlineLearning;

  public PerfEnforceDriver(final Server server, final String instancePath) {
    configurationPath = (Paths.get(instancePath, "perfenforce_files"));
    this.server = server;
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
      throw e;
      //throw new PerfEnforceException("Error while fetching files from S3");
    }
  }

  public void preparePSLA(List<PerfEnforceTableEncoding> tableList) throws Exception {
    this.tableList = tableList;
    fetchS3Files();

    PerfEnforceDataPreparation perfenforceDataPrepare = new PerfEnforceDataPreparation(server);

    for (PerfEnforceTableEncoding currentTable : tableList) {
      if (currentTable.type.equalsIgnoreCase("fact")) {
        perfenforceDataPrepare.ingestFact(currentTable);
      } else {
        perfenforceDataPrepare.ingestDimension(currentTable);
      }
      perfenforceDataPrepare.analyzeTable(currentTable);
    }

    perfenforceDataPrepare.collectSelectivities();
    PSLAManagerWrapper pslaManager = new PSLAManagerWrapper();
    pslaManager.generateQueries();
    perfenforceDataPrepare.collectFeaturesFromGeneratedQueries();

    pslaManager.generatePSLA();
    isDonePSLA = true;
  }

  /*
   * Start the tier and begin the new query session
   */

  public boolean isDonePSLA() {
    return isDonePSLA;
  }

  public void setTier(final int tier) {
    perfenforceOnlineLearning = new PerfEnforceOnlineLearning(server, tier);
  }

  public void findSLA(final String querySQL) throws PerfEnforceException, Exception {
    perfenforceOnlineLearning.findSLA(querySQL);
    perfenforceOnlineLearning.findBestClusterSize();
  }

  public void recordRealRuntime(final Double queryRuntime) throws PerfEnforceException {
    perfenforceOnlineLearning.recordRealRuntime(queryRuntime);
  }

  public PerfEnforceQueryMetadataEncoding getCurrentQuery() {
    return perfenforceOnlineLearning.getCurrentQuery();
  }

  public PerfEnforceQueryMetadataEncoding getPreviousQuery() {
    return perfenforceOnlineLearning.getPreviousQuery();
  }

  public int getClusterSize() {
    return perfenforceOnlineLearning.getClusterSize();
  }
}
