package edu.washington.escience.myriad.parallel;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rename threads by a prefix and a number. The numbering starts at 0.
 * */
public class RenamingThreadFactory implements ThreadFactory {

  private final String prefix;
  private final AtomicInteger seq;

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
