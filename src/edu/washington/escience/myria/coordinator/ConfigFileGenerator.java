package edu.washington.escience.myria.coordinator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.tools.MyriaConfiguration;

/**
 * A helper class used to generate worker config files.
 *
 */
public final class ConfigFileGenerator {

  /**
   *
   * @param configFileName config file name.
   * @param location place to create worker config files.
   * @throws ConfigFileException error when creating the config file.
   */
  public static void makeWorkerConfigFiles(final String configFileName, final String location)
      throws ConfigFileException {
    MyriaConfiguration config = MyriaConfiguration.loadWithDefaultValues(configFileName);
    for (int id : config.getWorkerIds()) {
      makeOneWorkerConfigFile(config, id, location);
    }
  }

  /**
   * Creates a worker config file.
   *
   * @param config the parsed configuration.
   * @param workerId the worker whose catalog is being creating.
   * @param path the place to save the worker config file.
   * @throws ConfigFileException error when creating the config file.
   */
  public static File makeOneWorkerConfigFile(
      final MyriaConfiguration config, final int workerId, final String path)
      throws ConfigFileException {

    /* Create the config file. */
    MyriaConfiguration wc = MyriaConfiguration.newConfiguration();

    /* Add all nodes. */
    wc.setValue(
        "master", MyriaConstants.MASTER_ID + "", config.getHostPort(MyriaConstants.MASTER_ID));
    for (int id : config.getWorkerIds()) {
      wc.setValue("workers", id + "", config.getHostPort(id));
    }

    /* Set configuration values if provided in config. */
    wc.setValue("deployment", MyriaSystemConfigKeys.DEPLOYMENT_PATH, config.getPath(workerId));
    wc.setValue(
        "deployment",
        MyriaSystemConfigKeys.DESCRIPTION,
        config.getRequired("deployment", MyriaSystemConfigKeys.DESCRIPTION));
    wc.setValue(
        "deployment",
        MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_SYSTEM,
        config.getOptional("deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_SYSTEM));
    wc.setValue(
        "deployment",
        MyriaSystemConfigKeys.USERNAME,
        config.getOptional("deployment", MyriaSystemConfigKeys.USERNAME));
    wc.setValue(
        "deployment",
        MyriaSystemConfigKeys.ADMIN_PASSWORD,
        config.getOptional("deployment", MyriaSystemConfigKeys.ADMIN_PASSWORD));
    wc.setValue(
        "deployment",
        MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_NAME,
        config.getWorkerDatabaseName(workerId));
    wc.setValue(
        "deployment",
        MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_PASSWORD,
        config.getOptional("deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_PASSWORD));
    wc.setValue(
        "deployment",
        MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_PORT,
        config.getOptional("deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_PORT));
    wc.setValue("runtime", MyriaSystemConfigKeys.WORKER_IDENTIFIER, workerId + "");
    config.copyRuntimeConfigs(wc);

    try {
      File workerDir = Files.createDirectories(Paths.get(path, "workers", workerId + "")).toFile();
      wc.write(
          new File(
              Paths.get(path, "workers", workerId + "", MyriaConstants.WORKER_CONFIG_FILE)
                  .toString()));
      return workerDir;
    } catch (IOException e) {
      throw new ConfigFileException(e);
    }
  }
}
