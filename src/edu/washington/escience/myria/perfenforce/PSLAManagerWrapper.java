/**
 *
 */
package edu.washington.escience.myria.perfenforce;

/**
 * A wrapper for the PSLAManager executable
 */
public class PSLAManagerWrapper {

  int tierSelected;

  /**
   * @param configFilePath
   * @return
   */
  public String generateQueries(final String configFilePath) {
    // here we can call via ProcessBuilder
    // Process process = new ProcessBuilder("C:\\PathToExe\\MyExe.exe","param1","param2").start();
    return null;
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
