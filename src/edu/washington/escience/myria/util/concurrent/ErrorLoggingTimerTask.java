package edu.washington.escience.myria.util.concurrent;

import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;

import edu.washington.escience.myria.util.JVMUtils;

/**
 * The Java {@link ScheduledExecutorService} suppress the subsequent execution of a {@link TimerTask} if any execution
 * of the task encounters an exception. This class captures all {@link Throwable}s and logs them. And all
 * {@link Exception}s are ignored but all {@link Error}s are re-thrown. In this way, the execution of the
 * {@link TimerTask}s will only get stopped by explicit cancel or shutdown.
 * */
public abstract class ErrorLoggingTimerTask extends TimerTask {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ErrorLoggingTimerTask.class.getName());

  /**
   * Process error.
   *
   * @param t the error.
   * */
  protected void exceptionCaught(final Throwable t) {
    if (LOGGER.isErrorEnabled()) {
      LOGGER.error("Exception in TimerTask: ", t);
    }
  }

  @Override
  public final void run() {
    try {
      runInner();
    } catch (Throwable e) {
      exceptionCaught(e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      } else if (e instanceof Error) {
        if (e instanceof OutOfMemoryError) {
          JVMUtils.shutdownVM(e);
        }
        throw (Error) e;
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
    }
  }

  /**
   * actual run code.
   *
   * @throws Exception if any error occurs.
   * */
  protected abstract void runInner() throws Exception;
}
