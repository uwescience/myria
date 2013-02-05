package edu.washington.escience.myriad.coordinator.catalog;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;

import edu.washington.escience.myriad.parallel.SocketInfo;

/**
 * A helper class used to make Catalogs. This will contain the creation code for all the Catalogs we use for
 * experiments, and also a bunch of useful helper functions that generally aide in the creation of Catalos.
 * 
 * @author dhalperi
 * 
 */
public final class CatalogMaker {

  /**
   * Used in Catalog creation.
   * 
   * @param args unused arguments.
   */
  public static void main(final String[] args) {
    // args[0]: directory name
    // args[1]: number of workers
    // args[2]: master, if specified, otherwise localhost
    // args[3-n]: workers, if specified, otherwise localhost
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    assert (args.length >= 2);
    try {
      if (args.length > 2) {
        makeNNodesParallelCatalog(args);
      } else {
        makeNNodesLocalParallelCatalog(args[0], Integer.parseInt(args[1]));
      }
    } catch (final IOException e) {
      System.err.println("Error creating catalog " + args[0] + ": " + e.getMessage());
    }
  }

  /**
   * Creates a Catalog for an N-node parallel system on the local machine and the corresponding WorkerCatalogs.
   * 
   * @param directoryName the directory where all the files should be stored.
   * @param n the number of nodes.
   * @throws IOException if the catalog file already exists.
   */
  public static void makeNNodesLocalParallelCatalog(final String directoryName, final int n) throws IOException {
    final int baseMasterPort = 8001;
    final int baseWorkerPort = 9001;
    final String[] args = new String[n + 3];
    args[0] = directoryName;
    args[1] = Integer.toString(n);
    args[2] = "localhost:" + baseMasterPort;
    for (int i = 0; i < n; ++i) {
      args[i + 3] = "localhost:" + (baseWorkerPort + i);
    }
    makeNNodesParallelCatalog(args);
  }

  /**
   * Creates a Catalog for an N-node parallel system on the local machine and the corresponding WorkerCatalogs, with
   * node addresses and ports specified.
   * 
   * @param directoryName the directory where all the files should be stored.
   * @param n the number of nodes.
   * @throws IOException if the catalog file already exists.
   */
  public static void makeNNodesParallelCatalog(final String[] args) throws IOException {
    final int n = args.length - 3;
    String baseDirectoryName = args[0];
    final String description = numberToEnglish(n) + "NodeParallel";
    List<SocketInfo> masters;
    Map<Integer, SocketInfo> workers;

    /* The server configuration. */
    try {
      Catalog c;
      final String catalogFileName = FilenameUtils.concat(baseDirectoryName, "master.catalog");
      final File catalogDir = new File(baseDirectoryName);
      while (!catalogDir.exists()) {
        catalogDir.mkdirs();
      }
      c = newCatalog(catalogFileName, description);
      c.addMaster(args[2]);
      for (int i = 0; i < n; ++i) {
        c.addWorker(args[i + 3]);
      }
      masters = c.getMasters();
      workers = c.getWorkers();
      c.close();
    } catch (final CatalogException e) {
      throw new RuntimeException(e);
    }

    /* Each worker's configuration. */
    for (final int workerId : workers.keySet()) {
      /* Start by making the directory for the worker */
      final String dirName = FilenameUtils.concat(baseDirectoryName, "worker_" + workerId);
      final File dir = new File(dirName);
      while (!dir.exists()) {
        dir.mkdirs();
      }

      final String sqliteDbName = FilenameUtils.concat(dirName, "worker_" + workerId + "_data.db");
      final String catalogName = FilenameUtils.concat(dirName, "worker.catalog");
      WorkerCatalog wc;
      try {
        /* Create the catalog. */
        try {
          wc = newWorkerCatalog(catalogName);
        } catch (final IOException e) {
          throw new RuntimeException("There is already a Catalog by that name", e);
        }

        /* Add any and all masters. */
        for (final SocketInfo s : masters) {
          wc.addMaster(s.toString());
        }

        /* Add any and all masters. */
        for (final Entry<Integer, SocketInfo> w : workers.entrySet()) {
          wc.addWorker(w.getKey(), w.getValue().toString());
        }

        /* Set up the other three configuration variables it uses. */
        wc.setConfigurationValue("worker.identifier", "" + workerId);
        wc.setConfigurationValue("worker.data.type", "sqlite");
        wc.setConfigurationValue("worker.data.sqlite.db", sqliteDbName);

      } catch (final CatalogException e) {
        throw new RuntimeException(e);
      }

      wc.close();
    }
  }

  /**
   * Helper utility that creates a new Catalog with a given filename and description.
   * 
   * @param filename specifies where the Catalog will be created.
   * @param description describes and names the Catalog. E.g., "twoNodeLocalParallel".
   * @return a fresh Catalog with the given description, stored in the path given by filename.
   * @throws CatalogException if there is an error in the backend database.
   * @throws IOException if the file already exists.
   * 
   *           TODO check the description can be a file basename, e.g., it has no / or space etc.
   */
  private static Catalog newCatalog(final String filename, final String description) throws CatalogException,
      IOException {
    Objects.requireNonNull(description);
    return Catalog.create(filename, description, false);
  }

  /**
   * Helper utility that creates a new WorkerCatalog with a given filename.
   * 
   * @param filename path to the WorkerCatalog database.
   * @return a fresh WorkerCatalog.
   * @throws CatalogException if there is an error in the backend database.
   * @throws IOException if the file already exists.
   * 
   *           TODO check the description can be a file basename, e.g., it has no / or space etc.
   */
  private static WorkerCatalog newWorkerCatalog(final String filename) throws CatalogException, IOException {
    Objects.requireNonNull(filename);
    return WorkerCatalog.create(filename, false);
  }

  /**
   * Converts a small number to its English spelling, and all other numbers to Strings followed by an underscore.
   * 
   * @param n the number.
   * @return its English spelling if 1 <= n <= 10. All other numbers are returned as Strings followed by an underscore.
   */
  private static String numberToEnglish(final int n) {
    switch (n) {
      case 1:
        return "one";
      case 2:
        return "two";
      case 3:
        return "three";
      case 4:
        return "four";
      case 5:
        return "five";
      case 6:
        return "six";
      case 7:
        return "seven";
      case 8:
        return "eight";
      case 9:
        return "nine";
      case 10:
        return "ten";
      default:
        return n + "_";
    }
  }

  /** Inaccessible. */
  private CatalogMaker() {
  }
}
