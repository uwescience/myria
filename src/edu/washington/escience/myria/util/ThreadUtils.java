package edu.washington.escience.myria.util;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to aid in Thread debugging.
 * 
 * @author dhalperi
 */
public final class ThreadUtils {

  /** Prevent construction of utility class. */
  private ThreadUtils() {
  }

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadUtils.class);

  /**
   * @return the currently-live threads.
   */
  public static Set<Thread> getCurrentThreads() {
    return Thread.getAllStackTraces().keySet();
  }

  /**
   * Print out the list of currently active threads.
   * 
   * @param tag a string printed before the message. For identifying calls to this function.
   */
  public static void printCurrentThreads(final String tag) {
    final Set<Thread> threadSet = getCurrentThreads();
    StringBuilder sb = new StringBuilder();
    sb.append(tag + ":" + threadSet.size() + " threads currently active.\n");
    sb.append('\t');
    for (final Thread t : threadSet) {
      sb.append(t.getId() + '[' + t.getName() + "], ");
    }
    sb.append('\n');
    LOGGER.warn(sb.toString());
  }
}
