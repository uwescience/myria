package edu.washington.escience.myria.util.concurrent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * The Java {@link ScheduledExecutorService} suppress the subsequent execution of a {@link TimerTask} if any execution
 * of the task encounters an exception. This {@link ThreadFactory} captures all {@link Throwable} and by default log it.
 * In this way, the execution of the {@link TimerTask}s will only get stopped by explicit cancel or shutdown.
 * */
public class TimerTaskThreadFactory extends RenamingThreadFactory {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TimerTaskThreadFactory.class
      .getName());

  /**
   * @param prefix the prefix.
   * */
  public TimerTaskThreadFactory(final String prefix) {
    super(prefix);
  }

  @Override
  public Thread newThread(final Runnable r) {
    Runnable rr = new Runnable() {
      @Override
      public void run() {
        try {
          r.run();
        } catch (Throwable eee) {
          // Suppress error throw to keep future scheduling of timer tasks
          exceptionCaught(r, eee);
        }
      }
    };
    return super.newThread(rr);
  }

  /**
   * Process error.
   * 
   * @param t the error.
   * @param r the {@link Runnable} that caused the error.
   * */
  protected void exceptionCaught(final Runnable r, final Throwable t) {
    if (LOGGER.isErrorEnabled()) {
      LOGGER.error("Exception in TimerTask: " + r, t);
    }
  }

}
