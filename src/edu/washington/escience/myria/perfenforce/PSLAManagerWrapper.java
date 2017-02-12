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
  final Path PSLAManagerExePath;

  /**
   * The constructor for the PSLAManagerWrapper class.
   * Initially sets up the path pointing to the executable for PSLAManager.
   */
  public PSLAManagerWrapper() {
    PSLAManagerExePath =
        PerfEnforceDriver.configurationPath
            .resolve("PSLAGeneration")
            .resolve("PSLAManager")
            .resolve("PSLAManager.exe");
  }

  /**
   * Runs the PSLAManager with the "-q" flag. It generates a set of queries based on the user's dataset.
   * @throws Exception if there is an error running the PSLAManager process
   */
  public void generateQueries() throws Exception {
    try {

      Process p =
          Runtime.getRuntime()
              .exec(
                  "mono "
                      + PSLAManagerExePath.toString()
                      + " -f "
                      + PerfEnforceDriver.configurationPath.resolve("PSLAGeneration").toString()
                      + " -q");

      p.waitFor();
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Runs the PSLAManager with the "-p" flag. It generates the resulting PSLA.
   * @throws Exception if there is an error running the PSLAManager process
   */
  public void generatePSLA() throws Exception {
    try {
      Process p =
          Runtime.getRuntime()
              .exec(
                  "mono "
                      + PSLAManagerExePath.toString()
                      + " -f "
                      + PerfEnforceDriver.configurationPath.resolve("PSLAGeneration").toString()
                      + " -p");

      p.waitFor();
    } catch (IOException e) {
      throw e;
    }
  }
}
