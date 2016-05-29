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

  int tierSelected;

  public PSLAManagerWrapper(final String configFilePath) {
    // download the executable and jars
    AmazonS3 conn = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
    conn.getObject(new GetObjectRequest("perfenforce", "PSLAManager.exe"),
        new File(configFilePath + "/PSLAManager.exe"));
    conn.getObject(new GetObjectRequest("perfenforce", "weka.jar"), new File(configFilePath + "/weka.jar"));

    // install mono

  }

  /**
   * @param configFilePath
   * @return
   */
  public void generateQueries(final Path configFilePath) {
    try {
      // might need to use mono
      Process process = new ProcessBuilder(configFilePath + "PSLAManager.exe -f " + configFilePath + "-q").start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 
   */
  public void generatePSLA() {
    // TODO Auto-generated method stub
    // Takes in query file list and stats and returns a PSLA
    // User will pick PSLA option
  }

  public int subsumption() {
    // subsumption call to PSLA? -- more like a "check PSLA"
    // returns a SLA number
    return 0;
  }

}
