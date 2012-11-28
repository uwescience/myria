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

  /** Inaccessible. */
  private CatalogMaker() {
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
   * Creates a Catalog for a 2-node parallel system on the local machine and the corresponding WorkerCatalogs.
   * 
   * @param directoryName the directory where all the files should be stored.
   */
  public static void makeTwoNodeLocalParallelCatalog(final String directoryName) {
    final String description = "twoNodeLocalParallel";
    String baseDirectoryName;
    if (directoryName == null) {
      baseDirectoryName = description;
    } else {
      baseDirectoryName = directoryName;
    }
    List<SocketInfo> masters;
    Map<Integer, SocketInfo> workers;

    /* The server configuration. */
    try {
      Catalog c;
      try {
        String catalogFileName = FilenameUtils.concat(baseDirectoryName, "master.catalog");
        File catalogDir = new File(baseDirectoryName);
        while (!catalogDir.exists()) {
          catalogDir.mkdirs();
        }
        c = newCatalog(catalogFileName, description);
      } catch (IOException e) {
        throw new RuntimeException("There is already a Catalog by that name", e);
      }
      c.addMaster("localhost:8001");
      c.addWorker("localhost:9001");
      c.addWorker("localhost:9002");
      /*
       * c.addMaster("rio.cs.washington.edu:8001"); c.addWorker("paris.cs.washington.edu:9001");
       * c.addWorker("seoul.cs.washington.edu:9001"); c.addWorker("sandiego.cs.washington.edu:9001");
       * c.addWorker("beijing.cs.washington.edu:9001"); c.addWorker("berlin.cs.washington.edu:9001");
       * c.addWorker("kyoto.cs.washington.edu:9001");
       */
      masters = c.getMasters();
      workers = c.getWorkers();
      c.close();
    } catch (CatalogException e) {
      throw new RuntimeException(e);
    }

    /* Worker1's configuration. */
    for (int workerId : workers.keySet()) {
      /* Start by making the directories for the worker */
      String dirName = FilenameUtils.concat(baseDirectoryName, "worker_" + workerId);
      String sqliteDirName = FilenameUtils.concat(dirName, "sqlite_dbs");
      String tmpDirName = FilenameUtils.concat(dirName, "tmp");
      /* .. the sqlite directory, and also on the way the worker directory. */
      File dir = new File(sqliteDirName);
      while (!dir.exists()) {
        dir.mkdirs();
      }
      /* .. the tmp directory. */
      dir = new File(tmpDirName);
      while (!dir.exists()) {
        dir.mkdirs();
      }

      String catalogName = FilenameUtils.concat(dirName, "worker.catalog");
      WorkerCatalog wc;
      try {
        /* Create the catalog. */
        try {
          wc = newWorkerCatalog(catalogName);
        } catch (IOException e) {
          throw new RuntimeException("There is already a Catalog by that name", e);
        }

        /* Add any and all masters. */
        for (SocketInfo s : masters) {
          wc.addMaster(s.getId());
        }

        /* Add any and all masters. */
        for (Entry<Integer, SocketInfo> w : workers.entrySet()) {
          wc.addWorker(w.getKey(), w.getValue().getId());
        }

        /* Set up the other three configuration variables it uses. */
        wc.setConfigurationValue("worker.identifier", "" + workerId);
        wc.setConfigurationValue("worker.data.sqlite.dir", sqliteDirName);
        wc.setConfigurationValue("worker.tmp.dir", tmpDirName);

      } catch (CatalogException e) {
        throw new RuntimeException(e);
      }

      wc.close();
    }
  }

  /**
   * Used in Catalog creation.
   * 
   * @param args unused arguments.
   */
  public static void main(final String[] args) {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    if (args.length > 0) {
      makeTwoNodeLocalParallelCatalog(args[0]);
    } else {
      makeTwoNodeLocalParallelCatalog(null);
    }
  }

}
