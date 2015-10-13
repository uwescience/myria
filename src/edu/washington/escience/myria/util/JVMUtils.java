package edu.washington.escience.myria.util;

/**
 * JVM util methods.
 */
public final class JVMUtils {

  /**
   * Shutdown the java virtual machine.
   */
  public static void shutdownVM() {
    System.exit(0);
  }

  /**
   * util classes are not instantiable.
   * */
  private JVMUtils() {}
}
