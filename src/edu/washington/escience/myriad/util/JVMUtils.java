package edu.washington.escience.myriad.util;

public class JVMUtils {

  private JVMUtils() {
  }

  /**
   * Shutdown the java virtual machine.
   */
  public static void shutdownVM() {
    System.exit(0);
  }
}
