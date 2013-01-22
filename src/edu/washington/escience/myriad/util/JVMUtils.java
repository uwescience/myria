package edu.washington.escience.myriad.util;

/**
 * JVM util methods.
 */
public final class JVMUtils {

  /**
   * util classes are not instantiable.
   * */
  private JVMUtils() {
  }

  /**
   * Shutdown the java virtual machine.
   */
  public static void shutdownVM() {
    System.exit(0);
  }
}
