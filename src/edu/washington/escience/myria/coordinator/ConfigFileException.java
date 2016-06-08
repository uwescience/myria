package edu.washington.escience.myria.coordinator;

/**
 *
 */
public class ConfigFileException extends Exception {

  /** */
  private static final long serialVersionUID = 1L;

  /**
   * @param cause the cause (which is saved for later by the Throwable.getCause() method).
   */
  public ConfigFileException(final Throwable cause) {
    super(cause);
  }
}
