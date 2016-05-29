/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A wrapper for the PSLAManager executable
 */
public class PSLAManagerWrapper {

  int tierSelected;

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
