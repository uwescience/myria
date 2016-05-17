package edu.washington.escience.myria.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.util.JVMUtils;

/**
 * This class cleanup all currently running user threads. It waits all these threads to finish within some given
 * timeout. If timeout, try interrupting them. If any thread is interrupted for a given number of times, stop waiting
 * and kill it directly.
 * */
public class ShutdownThreadCleaner extends Thread {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ShutdownThreadCleaner.class.getName());

  /**
   * In wait state for at most 5 seconds.
   * */
  public static final int DEFAULT_WAIT_MAXIMUM_MS = 5 * 1000;

  /**
   * @param waitBeforeInterruptMS wait this amount of milliseconds before interrupting
   * */
  private final int waitBeforeInterruptMS;

  /**
   * @param numInterruptBeforeKill number of interrupts before kill a thread
   * */
  private final int numInterruptBeforeKill;

  /**
   * Interrupt an unresponding thread for at most 3 times.
   * */
  public static final int DEFAULT_MAX_INTERRUPT_TIMES = 3;

  /**
   * The thread group executing the application's main function.
   * */
  private final ThreadGroup mainThreadGroup;

  /**
   * @param mainThreadGroup the thread group executing the application's main function
   * */
  public ShutdownThreadCleaner(final ThreadGroup mainThreadGroup) {
    super.setDaemon(true);
    this.mainThreadGroup = mainThreadGroup;
    waitBeforeInterruptMS = DEFAULT_WAIT_MAXIMUM_MS;
    numInterruptBeforeKill = DEFAULT_MAX_INTERRUPT_TIMES;
  }

  /**
   * @param mainThreadGroup the thread group executing the application's main function
   * @param waitBeforeInterruptMS wait this amount of milliseconds before interrupting
   * @param numInterruptBeforeKill number of interrupts before kill a thread
   * */
  public ShutdownThreadCleaner(
      final ThreadGroup mainThreadGroup,
      final int waitBeforeInterruptMS,
      final int numInterruptBeforeKill) {
    super.setDaemon(true);
    this.mainThreadGroup = mainThreadGroup;
    this.waitBeforeInterruptMS = waitBeforeInterruptMS;
    this.numInterruptBeforeKill = numInterruptBeforeKill;
  }

  /**
   * How many milliseconds a thread have been waited to get finish.
   * */
  private final HashMap<Thread, Integer> waitedForMS = new HashMap<Thread, Integer>();

  /**
   * Same to watedForMS, but keyed by thread name. The SQLiteQueue threads reincarnate themselves when any error occurs.
   * Using watedForMS won't capture the SQLiteQueue threads because the Thread instance will be recreated once they
   * receive an InterruptedException
   * */
  private final HashMap<String, Integer> waitedForMSThreadName = new HashMap<String, Integer>();

  /**
   * How many times a thread has been interrupted.
   * */
  private final HashMap<Thread, Integer> interruptTimes = new HashMap<Thread, Integer>();
  /**
   * The set of threads we have been waiting for the maximum MS, and so have decided to kill them directly.
   * */
  private final Set<Thread> abandonThreads = Sets.newSetFromMap(new HashMap<Thread, Boolean>());

  /**
   * utility method, add an integer v to the value of m[t] and return the new value. null key and value are taken ca of.
   *
   * @return the new value
   * @param <KEY> the map key type
   * @param m a map
   * @param t a thread
   * @param v the value
   * */
  private static <KEY> int addToMap(final Map<KEY, Integer> m, final KEY t, final int v) {
    Integer tt = m.get(t);
    if (tt == null) {
      tt = 0;
    }
    m.put(t, tt + v);
    return tt + v;
  }

  /**
   * utility method, get the value of m[t] . null key and value are taken care of.
   *
   * @param m a map
   * @param t a thread
   * @return the value
   * */
  private int getFromMap(final Map<Thread, Integer> m, final Thread t) {
    Integer tt = m.get(t);
    if (tt == null) {
      tt = 0;
    }
    return tt;
  }

  @SuppressWarnings("deprecation")
  @Override
  public final void run() {

    while (true) {
      Set<Thread> allThreads = Thread.getAllStackTraces().keySet();
      HashMap<Thread, Integer> nonSystemThreads = new HashMap<Thread, Integer>();
      for (final Thread t : allThreads) {
        if (t.getThreadGroup() != null
            && t.getThreadGroup() != mainThreadGroup
            && t.getThreadGroup() != mainThreadGroup.getParent()
            && t != Thread.currentThread()
            && !abandonThreads.contains(t)) {
          nonSystemThreads.put(t, 0);
        }
      }

      if (nonSystemThreads.isEmpty()) {
        if (abandonThreads.isEmpty()) {
          return;
        } else {
          JVMUtils.shutdownVM();
        }
      }

      try {
        Thread.sleep(MyriaConstants.SHORT_WAITING_INTERVAL_100_MS);
      } catch (InterruptedException e) {
        JVMUtils.shutdownVM();
      }

      for (final Thread t : nonSystemThreads.keySet()) {
        if (addToMap(
                waitedForMSThreadName, t.getName(), MyriaConstants.SHORT_WAITING_INTERVAL_100_MS)
            >= waitBeforeInterruptMS * numInterruptBeforeKill) {
          abandonThreads.add(t);
        } else if (addToMap(waitedForMS, t, MyriaConstants.SHORT_WAITING_INTERVAL_100_MS)
            > waitBeforeInterruptMS) {
          waitedForMS.put(t, 0);
          if (addToMap(interruptTimes, t, 1) > numInterruptBeforeKill) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "Thread {} have been interrupted for {} times. Kill it directly.",
                  t,
                  getFromMap(interruptTimes, t) - 1);
            }
            abandonThreads.add(t);
            t.stop();
          } else {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "Waited Thread {} to finish for {} seconds. I'll try interrupting it.",
                  t,
                  TimeUnit.MILLISECONDS.toSeconds(waitBeforeInterruptMS)
                      * getFromMap(interruptTimes, t));
            }
            t.interrupt();
          }
        }
      }
    }
  }
}
