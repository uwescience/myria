package edu.washington.escience.myria.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.coordinator.ConfigFileException;

public class MyriaLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(MyriaLauncher.class);

  public static void main(final String[] args)
      throws ConfigFileException, CatalogException, IOException, InterruptedException {
    LOGGER.debug("MyriaLauncher started with args {}, {}", args[0], args[1]);
    final String deploymentFile = args[0];
    final int oldPid = Integer.parseInt(args[1]);
    pollForExit(oldPid);
    DeploymentUtils.main(new String[] {deploymentFile, "--start-master"});
    DeploymentUtils.main(new String[] {deploymentFile, "--start-workers"});
  }

  private static void pollForExit(final int pidToExit) throws IOException, InterruptedException {
    boolean pidRunning;
    do {
      pidRunning = false;
      ProcessBuilder pb = new ProcessBuilder("ps", "-A");
      Process p = pb.start();
      BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
      // Skip first (header) line: "  PID TTY          TIME CMD"
      in.readLine();
      // Extract process IDs from lines of output
      // e.g. "  146 ?        00:03:45 pdflush"
      for (String line = in.readLine(); line != null; line = in.readLine()) {
        int currentPid = Integer.parseInt(line.trim().split("\\s+")[0]);
        if (currentPid == pidToExit) {
          LOGGER.debug("Process {} still running", currentPid);
          pidRunning = true;
          break;
        }
      }
      if (pidRunning) {
        Thread.sleep(1000);
      }
    } while (pidRunning);
  }
}
