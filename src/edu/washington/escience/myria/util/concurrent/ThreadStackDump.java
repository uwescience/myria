package edu.washington.escience.myria.util.concurrent;

/**
 * Dump thread stack.
 * */
public class ThreadStackDump extends Throwable {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * The thread who created the stack dump.
   * */
  private final Thread thread;

  /**
   * Create stack dump.
   * */
  public ThreadStackDump() {
    super("Stack dump for thread: " + Thread.currentThread().getName() + "\n");
    thread = Thread.currentThread();
  }

  /**
   * @return the thread whose stack gets dumped.
   * */
  public final Thread getThread() {
    return thread;
  }
}
