package edu.washington.escience.myria.cmd;

import edu.washington.escience.myria.daemon.MasterDaemon;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.parallel.Worker;

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
    MasterDaemon daemon = new MasterDaemon("leelee", 8753);
    daemon.start();
    Worker worker = new Worker("leelee/worker_1", QueryExecutionMode.NON_BLOCKING);
    worker.start();
  }
}
