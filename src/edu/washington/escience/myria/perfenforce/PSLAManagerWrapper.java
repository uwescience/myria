/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;

/**
 * A wrapper for the PSLAManager executable
 */
public class PSLAManagerWrapper {

  int tierSelected = 4; // hard coded for now, but should allow the user to pick

  public PSLAManagerWrapper(final String configFilePath) {
    // download the executable and jars
    AmazonS3 conn = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
    conn.getObject(new GetObjectRequest("perfenforce", "PSLAManager.exe"),
        new File(configFilePath + "/PSLAManager.exe"));
    conn.getObject(new GetObjectRequest("perfenforce", "Newtonsoft.Json.dll"), new File(configFilePath
        + "/Newtonsoft.Json.dll"));
    conn.getObject(new GetObjectRequest("perfenforce", "CommandLine.dll"),
        new File(configFilePath + "/CommandLine.dll"));
    conn.getObject(new GetObjectRequest("perfenforce", "weka.jar"), new File(configFilePath + "/weka.jar"));

  }

  /**
   * @param configFilePath
   * @return
   */
  public void generateQueries(final Path configFilePath, final int config) {
    try {
      Process process =
          new ProcessBuilder("mono " + configFilePath + "PSLAManager.exe -f " + configFilePath + config + "_Workers/"
              + "-q").start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 
   */
  public void generatePSLA(final Path configFilePath) {
    try {
      // might need to use mono
      Process process =
          new ProcessBuilder("mono " + configFilePath + "PSLAManager.exe -f " + configFilePath + "-p").start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public int subsumption() {
    // subsumption call to PSLA? -- more like a "check PSLA"
    // returns a SLA number
    return 0;
  }

}
