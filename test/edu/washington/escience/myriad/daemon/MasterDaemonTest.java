package edu.washington.escience.myriad.daemon;

import java.util.Set;

import org.junit.Test;
import org.restlet.resource.ClientResource;

import edu.washington.escience.myriad.util.ThreadUtils;

public class MasterDaemonTest {
  /* Test should timeout after 20 seconds. */
  public static final int TIMEOUT_MS = 20 * 1000;

  @Test(timeout = TIMEOUT_MS)
  public void testStartAndShutdown() throws Exception {
    /* Remember which threads were there when the test starts. */
    Set<Thread> startThreads = ThreadUtils.getCurrentThreads();

    /* Start the master. */
    MasterDaemon md = new MasterDaemon(new String[] { "twoNodeLocalParallel" });
    md.start();

    /* Stop the master. */
    md.stop();

    /* Wait for all threads that weren't there when we started to finish. */
    Set<Thread> doneThreads = ThreadUtils.getCurrentThreads();
    for (Thread t : doneThreads) {
      if (!startThreads.contains(t)) {
        t.join();
      }
    }
  }

  @Test(timeout = TIMEOUT_MS)
  public void testStartAndRestShutdown() throws Exception {
    /* Remember which threads were there when the test starts. */
    Set<Thread> startThreads = ThreadUtils.getCurrentThreads();

    /* Start the master. */
    MasterDaemon md = new MasterDaemon(new String[] { "twoNodeLocalParallel" });
    md.start();

    /* Stop the master. */
    ClientResource shutdownRest = new ClientResource("http://localhost:8753/server/shutdown");
    shutdownRest.get();
    shutdownRest.release();

    /* Wait for all threads that weren't there when we started to finish. */
    Set<Thread> doneThreads = ThreadUtils.getCurrentThreads();
    for (Thread t : doneThreads) {
      if (!startThreads.contains(t)) {
        t.join();
      }
    }
  }
}
