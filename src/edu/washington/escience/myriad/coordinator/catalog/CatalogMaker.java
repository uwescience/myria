package edu.washington.escience.myriad.coordinator.catalog;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.MyriaSystemConfigKeys;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
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
   * args[0]: directory name
   * 
   * args[1]: number of workers
   * 
   * args[2]: master, if specified, otherwise localhost
   * 
   * args[3-n]: workers, if specified, otherwise localhost
   * 
   * @param args contains the parameters necessary to start the catalog.
   * @throws IOException if there is an error creating the catalog.
   */
  public static void main(final String[] args) throws IOException {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Preconditions.checkArgument(args.length >= 2, "Usage: CatalogMaker <directory> <N> [master] [workers...]");
    try {
      if (args.length > 2) {
        makeNNodesParallelCatalog(args);
      } else {
        makeNNodesLocalParallelCatalog(args[0], Integer.parseInt(args[1]));
      }
    } catch (final IOException e) {
      System.err.println("Error creating catalog " + args[0] + ": " + e.getMessage());
      throw (e);
    } catch (final NumberFormatException e) {
      System.err.println("args[1] " + args[1] + " is not a number!");
      throw (e);
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
    HashMap<String, String> mc = new HashMap<String, String>();
    mc.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, "8001");
    HashMap<String, String> wc = new HashMap<String, String>();
    wc.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, "9001");
    makeNNodesLocalParallelCatalog(directoryName, n, mc, wc);
  }

  /**
   * Creates a Catalog for an N-node parallel system on the local machine and the corresponding WorkerCatalogs with
   * worker configurations.
   * 
   * @param directoryName the directory where all the files should be stored.
   * @param n the number of nodes.
   * @param masterConfigurations the configurations for the master
   * @param workerConfigurations the configurations for the worker
   * @throws IOException if the catalog file already exists.
   */
  public static void makeNNodesLocalParallelCatalog(final String directoryName, final int n,
      final Map<String, String> masterConfigurations, final Map<String, String> workerConfigurations)
      throws IOException {
    final String[] args = new String[n + 3];
    args[0] = directoryName;
    args[1] = Integer.toString(n);
    args[2] = "localhost:" + masterConfigurations.get(MyriaSystemConfigKeys.IPC_SERVER_PORT);
    final int baseWorkerPort = Integer.valueOf(workerConfigurations.get(MyriaSystemConfigKeys.IPC_SERVER_PORT));
    for (int i = 0; i < n; ++i) {
      args[i + 3] = "localhost:" + (baseWorkerPort + i);
    }
    makeNNodesParallelCatalog(args, masterConfigurations, workerConfigurations);
  }

  /**
   * Creates a Catalog for an N-node parallel system on the local machine and the corresponding WorkerCatalogs, with
   * node addresses and ports specified.
   * <p>
   * With worker configurations.
   * <p>
   * 
   * @param args the description and list of machines in this catalog.
   * @throws IOException if the catalog file already exists.
   * @param masterConfigurations the configurations for the master
   * @param workerConfigurations the configurations for the worker
   */
  public static void makeNNodesParallelCatalog(final String[] args, final Map<String, String> masterConfigurations,
      final Map<String, String> workerConfigurations) throws IOException {
    final int n = args.length - 3;
    String baseDirectoryName = args[0];
    final String description = numberToEnglish(n) + "NodeParallel";
    List<SocketInfo> masters;
    Map<Integer, SocketInfo> workers;

    /* The server configuration. */
    Catalog c = null;
    try {
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

      /* A simple test relation. */
      c.addRelationMetadata(RelationKey.of("test", "test", "testRelation"), new Schema(ImmutableList.of(Type.LONG_TYPE,
          Type.LONG_TYPE), ImmutableList.of("x", "y")));

      HashMap<String, String> configurationValues = new HashMap<String, String>(masterConfigurations);

      /* Add all missing configuration values to the map. */
      if (!masterConfigurations.containsKey(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)) {
        configurationValues.put(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES,
            MyriaConstants.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES_DEFAULT_VALUE + "");
      }
      if (!masterConfigurations.containsKey(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)) {
        configurationValues.put(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES,
            MyriaConstants.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES_DEFAULT_VALUE + "");
      }
      if (!masterConfigurations.containsKey(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY)) {
        configurationValues.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY,
            MyriaConstants.OPERATOR_INPUT_BUFFER_CAPACITY_DEFAULT_VALUE + "");
      }
      if (!masterConfigurations.containsKey(MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS)) {
        configurationValues.put(MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS,
            MyriaConstants.TCP_CONNECTION_TIMEOUT_MILLIS_DEFAULT_VALUE + "");
      }
      if (!masterConfigurations.containsKey(MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES)) {
        configurationValues.put(MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES,
            MyriaConstants.TCP_RECEIVE_BUFFER_SIZE_BYTES_DEFAULT_VALUE + "");
      }
      if (!masterConfigurations.containsKey(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES)) {
        configurationValues.put(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES,
            MyriaConstants.TCP_SEND_BUFFER_SIZE_BYTES_DEFAULT_VALUE + "");
      }
      c.setAllConfigurationValues(configurationValues);

      /* Close the master catalog. */
      c.close();
    } catch (final CatalogException e) {
      try {
        if (c != null) {
          c.close();
        }
      } catch (Exception e1) {
        assert true; /* Pass */
      }
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

        /* Build up a map of the worker configuration variables. */
        HashMap<String, String> configurationValues = new HashMap<String, String>(workerConfigurations);
        /* Three worker-specific values. */
        configurationValues.put(MyriaSystemConfigKeys.WORKER_IDENTIFIER, "" + workerId);
        configurationValues.put(MyriaSystemConfigKeys.WORKER_STORAGE_SYSTEM_TYPE, MyriaConstants.STORAGE_SYSTEM_SQLITE);
        configurationValues.put(MyriaSystemConfigKeys.WORKER_DATA_SQLITE_DB, sqliteDbName);
        /* All of the missing values from the other configurations. */
        if (!workerConfigurations.containsKey(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)) {
          configurationValues.put(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES, ""
              + MyriaConstants.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES_DEFAULT_VALUE);
        }
        if (!workerConfigurations.containsKey(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)) {
          configurationValues.put(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES, ""
              + MyriaConstants.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES_DEFAULT_VALUE);
        }
        if (!workerConfigurations.containsKey(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY)) {
          configurationValues.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY, ""
              + MyriaConstants.OPERATOR_INPUT_BUFFER_CAPACITY_DEFAULT_VALUE);
        }
        if (!workerConfigurations.containsKey(MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS)) {
          configurationValues.put(MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS, ""
              + MyriaConstants.TCP_CONNECTION_TIMEOUT_MILLIS_DEFAULT_VALUE);
        }
        if (!workerConfigurations.containsKey(MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES)) {
          configurationValues.put(MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES, ""
              + MyriaConstants.TCP_RECEIVE_BUFFER_SIZE_BYTES_DEFAULT_VALUE);
        }
        if (!workerConfigurations.containsKey(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES)) {
          configurationValues.put(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES, ""
              + MyriaConstants.TCP_SEND_BUFFER_SIZE_BYTES_DEFAULT_VALUE);
        }
        /* Set them all in the worker catalog. */
        wc.setAllConfigurationValues(configurationValues);
      } catch (final CatalogException e) {
        throw new RuntimeException(e);
      }
      wc.close();
    }
  }

  /**
   * Creates a Catalog for an N-node parallel system on the local machine and the corresponding WorkerCatalogs, with
   * node addresses and ports specified.
   * 
   * @param args the description and list of machines in this catalog.
   * @throws IOException if the catalog file already exists.
   */
  public static void makeNNodesParallelCatalog(final String[] args) throws IOException {
    Map<String, String> mc = Collections.emptyMap();
    Map<String, String> wc = Collections.emptyMap();
    makeNNodesParallelCatalog(args, mc, wc);
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
      default:
        return n + "_";
    }
  }

  /** Inaccessible. */
  private CatalogMaker() {
  }
}
