package edu.washington.escience.myriad.catalog;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;

/**
 * A helper class used to make Catalogs. This will contain the creation code for all the Catalogs we use for
 * experiments, and also a bunch of useful helper functions that generally aide in the creation of Catalos.
 * 
 * @author dhalperi
 * 
 */
public final class CatalogMaker {

  /** Inaccessible. */
  private CatalogMaker() {
  }

  /**
   * Helper utility that creates a new Catalog with a given filename and description.
   * 
   * @param description describes and names the Catalog. E.g., "twoNodeLocalParallel".
   * @return a fresh Catalog with the given description, stored in "catalogs/description.catalog"
   * @throws CatalogException if there is an error in the backend database.
   * @throws IOException if the file already exists.
   * 
   *           TODO check the description can be a file basename, e.g., it has no / or space etc.
   */
  private static Catalog newCatalog(final String description) throws CatalogException, IOException {

    final String fileName = FilenameUtils.concat("catalogs", description + ".catalog");
    return Catalog.create(fileName, description, false);
  }

  /**
   * Creates a Catalog for a 2-node parallel system on the local machine.
   */
  private static void makeTwoNodeLocalParallelCatalog() {
    final String description = "twoNodeLocalParallel";
    try {
      Catalog c = newCatalog(description);
      c.addServer("localhost:8001");
      c.addWorker("localhost:9001");
      c.addWorker("localhost:9002");
      c.close();
    } catch (CatalogException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException("There is already a Catalog by that name", e);
    }
  }

  /**
   * Used in Catalog creation.
   * 
   * @param args unused arguments.
   */
  public static void main(final String[] args) {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    makeTwoNodeLocalParallelCatalog();
  }

}
