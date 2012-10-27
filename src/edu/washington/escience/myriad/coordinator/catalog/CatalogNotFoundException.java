package edu.washington.escience.myriad.coordinator.catalog;

/**
 * Thrown when a catalog already exists.
 * 
 * @author dhalperi
 * 
 */
public class CatalogNotFoundException extends CatalogException {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param message the detail message (which is saved for later retrieval by the Throwable.getMessage() method).
   */
  public CatalogNotFoundException(final String message) {
    super(message);
  }
}
