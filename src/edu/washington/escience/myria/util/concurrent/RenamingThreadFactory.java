package edu.washington.escience.myria.util.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rename threads by a prefix and a number. The numbering starts at 0.
 * */
public class RenamingThreadFactory implements ThreadFactory {

  /**
   * the prefix.
   * */
  private final String prefix;
  /**
   * The atomic suffix number generator.
   * */
  private final AtomicInteger seq;

  /**
   * @param prefix the prefix.
   * */
  public RenamingThreadFactory(final String prefix) {
    this.prefix = prefix;
    seq = new AtomicInteger(0);
  }

  @Override
  public Thread newThread(final Runnable r) {
    return new Thread(prefix + "#" + seq.getAndIncrement()) {
      @Override
      public void run() {
        r.run();
      }
    };
  }
}
