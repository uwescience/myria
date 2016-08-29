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

  public PSLAManagerWrapper() {
    PSLAManagerExePath =
        PerfEnforceDriver.configurationPath.resolve("PSLAManager").resolve("PSLAManager.exe");
  }

  public void generateQueries() {
    try {
      Process p =
          Runtime.getRuntime()
              .exec(
                  "mono "
                      + PSLAManagerExePath.toString()
                      + " -f "
                      + PerfEnforceDriver.configurationPath.toString()
                      + " -q");
      p.waitFor();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void generatePSLA() {
    try {
      Process p =
          Runtime.getRuntime()
              .exec(
                  "mono "
                      + PSLAManagerExePath.toString()
                      + " -f "
                      + PerfEnforceDriver.configurationPath.toString()
                      + " -p");
      p.waitFor();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}
