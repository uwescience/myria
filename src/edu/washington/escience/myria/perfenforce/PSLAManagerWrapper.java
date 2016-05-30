/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
  String PSLAManagerPath;

  public PSLAManagerWrapper(final String configFilePath) throws IOException {
    // download the executable and jars
    AmazonS3 conn = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());

    // download necessary files for PSLAManager
    String currentLine = "";
    BufferedReader br = new BufferedReader(new FileReader(configFilePath + "filesToFetch.txt"));
    while ((currentLine = br.readLine()) != null) {
      conn.getObject(new GetObjectRequest("perfenforce", currentLine), new File(configFilePath + "/" + currentLine));
    }

    PSLAManagerPath = configFilePath + "/PSLAManager/";
  }

  /**
   * @param configFilePath
   * @return
   */
  public void generateQueries(final String configFilePath, final int config) {
    try {
      Process p =
          Runtime.getRuntime().exec(
              "mono " + PSLAManagerPath + "PSLAManager.exe -f " + configFilePath + config + "_Workers/" + " -q");
      p.waitFor();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * 
   */
  public void generatePSLA(final Path configFilePath) {
    try {
      Process p =
          Runtime.getRuntime().exec(
              "mono " + PSLAManagerPath + "PSLAManager.exe -f " + configFilePath + "_Workers/" + " -p");
      p.waitFor();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public int subsumption() {
    // subsumption call to PSLA? -- more like a "check PSLA"
    // returns a SLA number
    return 0;
  }

}
