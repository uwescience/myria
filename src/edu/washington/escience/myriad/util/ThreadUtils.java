package edu.washington.escience.myriad.util;

import java.util.Set;

public class ThreadUtils {

  /**
   * Print out the list of currently active threads.
   * 
   * @param tag a string printed before the message. For identifying calls to this function.
   */
  public static void printCurrentThreads(final String tag) {
    final Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    System.out.println(tag + ":" + threadSet.size() + " threads currently active.");
    System.out.print("\t");
    for (final Thread t : threadSet) {
      System.out.print(t.getId() + ", ");
    }
    System.out.println();
  }
}
