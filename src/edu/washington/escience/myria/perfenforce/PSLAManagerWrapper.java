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
  final Path PSLAManagerPath;

  public PSLAManagerWrapper() {
    PSLAManagerPath = PerfEnforceDriver.configurationPath.resolve("PLSAManager");
  }

  public void generateQueries() {
    try {
      Process p =
          Runtime.getRuntime()
              .exec(
                  "mono "
                      + PSLAManagerPath.toString()
                      + "PSLAManager.exe -f "
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
                      + PSLAManagerPath.toString()
                      + "PSLAManager.exe -f "
                      + PerfEnforceDriver.configurationPath.toString()
                      + " -p");
      p.waitFor();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}
