package edu.washington.escience.myriad.cmd;

import edu.washington.escience.myriad.daemon.MasterDaemon;
import edu.washington.escience.myriad.parallel.QueryExecutionMode;
import edu.washington.escience.myriad.parallel.Worker;

/**
 * A simple utility class. Modify it when you want to run both the Master and the Worker in the same process for
 * profiling reasons.
 * 
 * @author dhalperi
 * 
 */
public final class RunMyriaForProfiling {

  /** Disable construction. */
  private RunMyriaForProfiling() {
  }

  /**
   * @param args arguments.
   * @throws Exception exception.
   */
  public static void main(final String[] args) throws Exception {
    MasterDaemon daemon = new MasterDaemon(new String[] { "leelee", "8753" });
    daemon.start();
    Worker worker = new Worker("leelee/worker_1", QueryExecutionMode.NON_BLOCKING);
    worker.start();
  }
}
