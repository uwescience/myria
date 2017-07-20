package edu.washington.escience.myria.util.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import edu.washington.escience.myria.util.JVMUtils;

/**
 * Rename threads by a prefix and a number. The numbering starts at 0.
 */
public class RenamingThreadFactory implements ThreadFactory {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(RenamingThreadFactory.class);

  /**
   * the prefix.
   */
  private final String prefix;
  /**
   * The atomic suffix number generator.
   */
  private final AtomicInteger seq;

  /**
   * @param prefix the prefix.
   */
  public RenamingThreadFactory(final String prefix) {
    this.prefix = prefix;
    seq = new AtomicInteger(0);
  }

  @Override
  public Thread newThread(final Runnable r) {
    Thread t =
        new Thread(prefix + "#" + seq.getAndIncrement()) {
          @Override
          public void run() {
            r.run();
          }
        };
    t.setUncaughtExceptionHandler(
        new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(final Thread t, final Throwable e) {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error("Uncaught exception in thread: " + t, e);
            }
            if (e instanceof OutOfMemoryError) {
              JVMUtils.shutdownVM(e);
            }
          }
        });
    return t;
  }
}
