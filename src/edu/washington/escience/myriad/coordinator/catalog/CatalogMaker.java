package edu.washington.escience.myriad.coordinator.catalog;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.MyriaSystemConfigKeys;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.tool.MyriaConfigurationReader;

/**
 * A helper class used to make Catalogs. This will contain the creation code for all the Catalogs we use for
 * experiments, and also a bunch of useful helper functions that generally aide in the creation of Catalos.
 * 
 * @author dhalperi
 * 
 */
public final class CatalogMaker {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CatalogMaker.class);

  /** The reader. */
  private static final MyriaConfigurationReader READER = new MyriaConfigurationReader();

  /**
   * Used in Catalog creation. args[0]: directory name args[1]: path to the config file.
   * 
   * @param args contains the parameters necessary to start the catalog.
   * @throws IOException if there is an error creating the catalog.
   */
  public static void main(final String[] args) throws IOException {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Preconditions.checkArgument(args.length >= 1,
        "Usage: CatalogMaker <config_file> <optional: catalog_location> <optional: overwrite>");
    try {
      makeNNodesParallelCatalog(args);
    } catch (final IOException e) {
      System.err.println("Error creating catalog " + args[0] + ": " + e.getMessage());
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

    final String[] args = new String[2];
    args[1] = directoryName;
    File temp = File.createTempFile("localMyriaConfig", ".cfg");
    BufferedWriter writer = new BufferedWriter(new FileWriter(temp));
    writer.write("[deployment]\n");
    writer.write("[master]\n");
    writer.write("0 = localhost:" + masterConfigurations.get(MyriaSystemConfigKeys.IPC_SERVER_PORT) + "\n");
    writer.write("[workers]\n");
    final int baseWorkerPort = Integer.valueOf(workerConfigurations.get(MyriaSystemConfigKeys.IPC_SERVER_PORT));
    for (int i = 1; i <= n; ++i) {
      writer.write(i + " = localhost:" + (baseWorkerPort + i) + "\n");
    }
    writer.close();
    args[0] = temp.getAbsolutePath();
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

    /* The server configuration. */
    final String configFileName = args[0];
    Map<String, HashMap<String, String>> config = READER.load(configFileName);
    MasterCatalog c = null;
    try {
      String catalogLocation;
      if (args.length > 1) {
        catalogLocation = args[1];
      } else {
        catalogLocation = config.get("deployment").get("name");
      }
      boolean overwrite = true;
      if (args.length > 2) {
        overwrite = Boolean.parseBoolean(args[2]);
      }
      final String catalogFileName = FilenameUtils.concat(catalogLocation, "master.catalog");
      final File catalogDir = new File(catalogLocation);
      while (!catalogDir.exists()) {
        catalogDir.mkdirs();
      }
      c = newMasterCatalog(catalogFileName, overwrite);
      final Map<String, String> masters = config.get("master");
      for (final String id : masters.keySet()) {
        c.addMaster(masters.get(id));
      }
      final Map<String, String> workers = config.get("workers");
      for (final String id : workers.keySet()) {
        c.addWorker(Integer.parseInt(id), workers.get(id));
      }

      /* A simple test relation. */
      c.addRelationMetadata(RelationKey.of("test", "test", "testRelation"), new Schema(ImmutableList.of(Type.LONG_TYPE,
          Type.LONG_TYPE), ImmutableList.of("x", "y")));

      HashMap<String, String> configurationValues = new HashMap<String, String>(masterConfigurations);
      MyriaSystemConfigKeys.addDeploymentKeysFromConfigFile(configurationValues, config.get("deployment"));

      /* Add all missing default configuration values to the map. */
      MyriaSystemConfigKeys.addDefaultConfigKeys(configurationValues);

      /* The master-specific values. */
      configurationValues.put(MyriaSystemConfigKeys.DEPLOYMENT_FILE, configFileName);

      c.setAllConfigurationValues(configurationValues);

      /* Each worker's configuration. */
      for (final String workerId : workers.keySet()) {
        makeOneWorkerCatalog(workerId, catalogLocation, config, workerConfigurations, overwrite);
      }
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
  }

  /**
   * Creates a WorkerCatalog.
   * 
   * @param workerId the worker whose catalog is being creating.
   * @param catalogLocation directory name.
   * @param config the parsed configuration.
   * @param workerConfigurations worker configuration.
   * @param overwrite true/false if want to overwrite old catalog.
   */
  public static void makeOneWorkerCatalog(final String workerId, final String catalogLocation,
      final Map<String, HashMap<String, String>> config, final Map<String, String> workerConfigurations,
      final boolean overwrite) {

    /* Start by making the directory for the worker */
    final String dirName = FilenameUtils.concat(catalogLocation, "worker_" + workerId);
    final File dir = new File(dirName);
    while (!dir.exists()) {
      dir.mkdirs();
    }

    final String catalogName = FilenameUtils.concat(dirName, "worker.catalog");
    WorkerCatalog wc;
    try {
      /* Create the catalog. */
      try {
        wc = newWorkerCatalog(catalogName, overwrite);
      } catch (final IOException e) {
        throw new RuntimeException("There is already a Catalog by that name", e);
      }

      /* Add any and all masters. */
      final Map<String, String> masters = config.get("master");
      for (final String id : masters.keySet()) {
        wc.addMaster(masters.get(id));
      }
      final Map<String, String> workers = config.get("workers");
      for (final String id : workers.keySet()) {
        wc.addWorker(Integer.parseInt(id), workers.get(id));
      }

      /* Build up a map of the worker configuration variables. */
      HashMap<String, String> configurationValues = new HashMap<String, String>(workerConfigurations);
      configurationValues.put(MyriaSystemConfigKeys.WORKING_DIRECTORY, config.get("paths").get(workerId));
      MyriaSystemConfigKeys.addDeploymentKeysFromConfigFile(configurationValues, config.get("deployment"));

      /* Add all missing default configuration values to the map. */
      MyriaSystemConfigKeys.addDefaultConfigKeys(configurationValues);

      /* Three worker-specific values. */
      String description = config.get("deployment").get("name");
      String sqliteDbName = "";
      if (description != null) {
        sqliteDbName = FilenameUtils.concat(description, "worker_" + workerId);
        sqliteDbName = FilenameUtils.concat(sqliteDbName, "worker_" + workerId + "_data.db");
      } else {
        sqliteDbName = FilenameUtils.concat(dirName, "worker_" + workerId + "_data.db");
      }
      configurationValues.put(MyriaSystemConfigKeys.WORKER_IDENTIFIER, "" + workerId);
      configurationValues.put(MyriaSystemConfigKeys.WORKER_STORAGE_SYSTEM_TYPE, MyriaConstants.STORAGE_SYSTEM_SQLITE);
      configurationValues.put(MyriaSystemConfigKeys.WORKER_DATA_SQLITE_DB, sqliteDbName);

      /* Set them all in the worker catalog. */
      wc.setAllConfigurationValues(configurationValues);
    } catch (final CatalogException e) {
      throw new RuntimeException(e);
    }
    wc.close();
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
   * @param overwrite true/false if want to overwrite old catalog.
   * @return a fresh Catalog with the given description, stored in the path given by filename.
   * @throws CatalogException if there is an error in the backend database.
   * @throws IOException if the file already exists.
   * 
   *           TODO check the description can be a file basename, e.g., it has no / or space etc.
   */
  private static MasterCatalog newMasterCatalog(final String filename, final boolean overwrite)
      throws CatalogException, IOException {
    return MasterCatalog.create(filename, overwrite);
  }

  /**
   * Helper utility that creates a new WorkerCatalog with a given filename.
   * 
   * @param filename path to the WorkerCatalog database.
   * @param overwrite true/false if want to overwrite old catalog.
   * @return a fresh WorkerCatalog.
   * @throws CatalogException if there is an error in the backend database.
   * @throws IOException if the file already exists.
   * 
   *           TODO check the description can be a file basename, e.g., it has no / or space etc.
   */
  private static WorkerCatalog newWorkerCatalog(final String filename, final boolean overwrite)
      throws CatalogException, IOException {
    Objects.requireNonNull(filename);
    return WorkerCatalog.create(filename, overwrite);
  }

  /** Inaccessible. */
  private CatalogMaker() {
  }
}
