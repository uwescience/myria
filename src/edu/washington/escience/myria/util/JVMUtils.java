package edu.washington.escience.myria.util;

/**
 * JVM util methods.
 */
public final class JVMUtils {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(JVMUtils.class);

  /**
   * Shutdown the java virtual machine.
   */
  public static void shutdownVM() {
    System.exit(0);
  }

  public static void shutdownVM(final Throwable e) {
    LOGGER.error("System will exit due to " + e);
    System.exit(1);
  }

  /**
   * util classes are not instantiable.
   * */
  private JVMUtils() {}
}
