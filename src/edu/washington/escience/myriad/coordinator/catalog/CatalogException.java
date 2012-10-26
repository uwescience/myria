/**
 * 
 */
package edu.washington.escience.myriad.coordinator.catalog;

/**
 * @author dhalperi
 * 
 */
public class CatalogException extends Exception {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param message the detail message (which is saved for later retrieval by the Throwable.getMessage() method).
   */
  public CatalogException(final String message) {
    super(message);
  }

  /**
   * @param cause the cause (which is saved for later by the Throwable.getCause() method). (A null value is permitted,
   *          and indicates that the cause is nonexistent or unknown.)
   */
  public CatalogException(final Throwable cause) {
    super(cause);
  }

  /**
   * @param message the detail message (which is saved for later retrieval by the Throwable.getMessage() method).
   * @param cause the cause (which is saved for later by the Throwable.getCause() method). (A null value is permitted,
   *          and indicates that the cause is nonexistent or unknown.)
   */
  public CatalogException(final String message, final Throwable cause) {
    super(message, cause);
  }

}