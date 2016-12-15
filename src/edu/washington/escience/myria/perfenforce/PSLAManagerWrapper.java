/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.io.IOUtils;

/**
 * A wrapper for the PSLAManager executable
 */
public class PSLAManagerWrapper {
  final Path PSLAManagerExePath;

  public PSLAManagerWrapper() {
    PSLAManagerExePath =
        PerfEnforceDriver.configurationPath.resolve("PSLAGeneration").resolve("PSLAManager").resolve("PSLAManager.exe");
  }

  public void generateQueries() throws Exception {
    try {

      Process p =
          Runtime.getRuntime()
              .exec(
                  "mono "
                      + PSLAManagerExePath.toString()
                      + " -f "
                      + PerfEnforceDriver.configurationPath.toString()
                      + " -q");

      System.out.println(IOUtils.toString(p.getErrorStream()));

      p.waitFor();
    } catch (IOException e) {
      throw e;
    }
  }

  public void generatePSLA() throws Exception {
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
    } catch (IOException e) {
      throw e;
    }
  }
}
