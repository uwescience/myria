package edu.washington.escience.myria.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.OptionalParameter;
import org.ini4j.ConfigParser;
import org.ini4j.ConfigParser.InterpolationException;
import org.ini4j.ConfigParser.InterpolationMissingOptionException;
import org.ini4j.ConfigParser.NoOptionException;
import org.ini4j.ConfigParser.NoSectionException;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.api.MyriaApiConstants;
import edu.washington.escience.myria.coordinator.ConfigFileException;

/** The class to read Myria configuration file, e.g. deployment.cfg. */
public final class MyriaConfigurationParser {

  // this is a static utility class
  private MyriaConfigurationParser() {}

  /**
   * load the config file.
   *
   * @param filename filename.
   * @return parsed mapping from sections to keys to values.
   * @throws ConfigFileException if error occurred when parsing the config file
   * */
  public static Configuration loadConfiguration(final String filename) throws ConfigFileException {
    final ConfigParser parser = new ConfigParser();
    try {
      parser.read(new File(filename));
    } catch (IOException e) {
      throw new ConfigFileException(e);
    }
    ConfigurationModule cm = MyriaGlobalConfigurationModule.CONF;
    cm = setGlobalConfVariables(parser, cm);
    cm = setWorkerConfs(parser, cm);
    cm = setJvmOptions(parser, cm);
    return cm.build();
  }

  private static ConfigurationModule setGlobalConfVariables(
      final ConfigParser parser, final ConfigurationModule cm) throws ConfigFileException {

    // Required parameters
    ConfigurationModule conf =
        cm.set(
                MyriaGlobalConfigurationModule.INSTANCE_NAME,
                getRequired(parser, "deployment", MyriaSystemConfigKeys.DESCRIPTION))
            .set(
                MyriaGlobalConfigurationModule.DEFAULT_INSTANCE_PATH,
                getPath(parser, MyriaConstants.MASTER_ID))
            .set(
                MyriaGlobalConfigurationModule.MASTER_HOST,
                getHostname(parser, MyriaConstants.MASTER_ID))
            .set(
                MyriaGlobalConfigurationModule.MASTER_RPC_PORT,
                getPort(parser, MyriaConstants.MASTER_ID))
            .set(
                MyriaGlobalConfigurationModule.PERSIST_URI,
                getRequired(parser, "persist", MyriaSystemConfigKeys.PERSIST_URI));

    // Optional parameters
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.STORAGE_DBMS,
            getOptional(
                parser, "deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_SYSTEM));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.DEFAULT_STORAGE_DB_NAME,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_NAME));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.DEFAULT_STORAGE_DB_USERNAME,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.USERNAME));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.DEFAULT_STORAGE_DB_PASSWORD,
            getOptional(
                parser, "deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_PASSWORD));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.DEFAULT_STORAGE_DB_PORT,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_PORT));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.REST_API_PORT,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.REST_PORT));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.API_ADMIN_PASSWORD,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.ADMIN_PASSWORD));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.USE_SSL,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.SSL));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.SSL_KEYSTORE_PATH,
            getOptional(parser, "deployment", MyriaApiConstants.MYRIA_API_SSL_KEYSTORE));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.SSL_KEYSTORE_PASSWORD,
            getOptional(parser, "deployment", MyriaApiConstants.MYRIA_API_SSL_KEYSTORE_PASSWORD));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.ENABLE_DEBUG,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.DEBUG));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.MASTER_NUMBER_VCORES,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.MASTER_NUMBER_VCORES));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.WORKER_NUMBER_VCORES,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.WORKER_NUMBER_VCORES));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.DRIVER_MEMORY_QUOTA_GB,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.DRIVER_MEMORY_QUOTA_GB));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.MASTER_MEMORY_QUOTA_GB,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.MASTER_MEMORY_QUOTA_GB));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.WORKER_MEMORY_QUOTA_GB,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.WORKER_MEMORY_QUOTA_GB));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.MASTER_JVM_HEAP_SIZE_MIN_GB,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.MASTER_JVM_HEAP_SIZE_MIN_GB));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.WORKER_JVM_HEAP_SIZE_MIN_GB,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.WORKER_JVM_HEAP_SIZE_MIN_GB));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.MASTER_JVM_HEAP_SIZE_MAX_GB,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.MASTER_JVM_HEAP_SIZE_MAX_GB));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.WORKER_JVM_HEAP_SIZE_MAX_GB,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.WORKER_JVM_HEAP_SIZE_MAX_GB));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES,
            getOptional(
                parser,
                "deployment",
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES,
            getOptional(
                parser,
                "deployment",
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.OPERATOR_INPUT_BUFFER_CAPACITY,
            getOptional(
                parser, "deployment", MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER,
            getOptional(
                parser, "deployment", MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.TCP_CONNECTION_TIMEOUT_MILLIS,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.TCP_SEND_BUFFER_SIZE_BYTES,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES));
    conf =
        setOptional(
            conf,
            MyriaGlobalConfigurationModule.TCP_RECEIVE_BUFFER_SIZE_BYTES,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES));

    return conf;
  }

  private static ConfigurationModule setWorkerConfs(
      final ConfigParser parser, final ConfigurationModule cm) throws ConfigFileException {
    ConfigurationModule conf = cm;
    for (Integer workerId : getWorkerIds(parser)) {
      Configuration workerConfig =
          MyriaWorkerConfigurationModule.CONF
              .set(MyriaWorkerConfigurationModule.WORKER_ID, workerId + "")
              .set(MyriaWorkerConfigurationModule.WORKER_HOST, getHostname(parser, workerId))
              .set(MyriaWorkerConfigurationModule.WORKER_PORT, getPort(parser, workerId))
              .set(MyriaWorkerConfigurationModule.WORKER_JVM_PORT, getJVMPort(parser, workerId))
              .set(MyriaWorkerConfigurationModule.WORKER_STATS_PORT, getStatsPort(parser, workerId))
              .set(
                  MyriaWorkerConfigurationModule.WORKER_STORAGE_DB_NAME,
                  getWorkerDatabaseName(parser, workerId))
              .set(MyriaWorkerConfigurationModule.WORKER_FILESYSTEM_PATH, getPath(parser, workerId))
              .build();
      String serializedWorkerConfig = new AvroConfigurationSerializer().toString(workerConfig);
      conf = conf.set(MyriaGlobalConfigurationModule.WORKER_CONF, serializedWorkerConfig);
    }
    return conf;
  }

  private static ConfigurationModule setJvmOptions(
      final ConfigParser parser, final ConfigurationModule cm) throws ConfigFileException {
    ConfigurationModule conf = cm;
    final String options = getOptional(parser, "runtime", MyriaSystemConfigKeys.JVM_OPTIONS);
    if (options != null) {
      final List<String> optionList = Arrays.asList(options.split(" "));
      for (final String option : optionList) {
        conf = conf.set(MyriaGlobalConfigurationModule.JVM_OPTIONS, option);
      }
    }
    return conf;
  }

  /**
   *
   * @param nodeId the node ID
   * @return working directory
   * @throws ConfigFileException if error occurred when getting the value
   */
  private static String getPath(final ConfigParser parser, final int nodeId)
      throws ConfigFileException {
    if (nodeId != MyriaConstants.MASTER_ID) {
      // worker, check if its path is specified
      String[] tmp = getRequired(parser, "workers", nodeId + "").split(":");
      if (tmp.length > 2) {
        return tmp[2];
      }
    }
    return getRequired(parser, "deployment", MyriaSystemConfigKeys.DEPLOYMENT_PATH);
  }

  /**
   *
   * @param workerId the worker ID
   * @return the database name
   * @throws ConfigFileException if error occurred when getting the value
   */
  private static String getWorkerDatabaseName(final ConfigParser parser, final int workerId)
      throws ConfigFileException {
    String[] tmp = getRequired(parser, "workers", workerId + "").split(":");
    if (tmp.length > 3) {
      // use the value specified for this worker
      return tmp[3];
    }
    // otherwise the default one
    return getOptional(parser, "deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_NAME);
  }

  /**
   *
   * @param nodeId the worker/master ID
   * @return the hostname
   * @throws ConfigFileException if error occurred when getting the value
   */
  private static String getHostname(final ConfigParser parser, final int nodeId)
      throws ConfigFileException {
    if (nodeId == MyriaConstants.MASTER_ID) {
      return getRequired(parser, "master", nodeId + "").split(":")[0];
    } else {
      return getRequired(parser, "workers", nodeId + "").split(":")[0];
    }
  }

  /**
   *
   * @param nodeId the worker/master ID
   * @return the port number
   * @throws ConfigFileException if error occurred when getting the value
   */
  private static int getPort(final ConfigParser parser, final int nodeId)
      throws ConfigFileException {
    if (nodeId == MyriaConstants.MASTER_ID) {
      return Integer.parseInt(getRequired(parser, "master", nodeId + "").split(":")[1]);
    } else {
      return Integer.parseInt(getRequired(parser, "workers", nodeId + "").split(":")[1]);
    }
  }

  /**
   *
   * @param nodeId the worker/master ID
   * @return the port number
   * @throws ConfigFileException if error occurred when getting the value
   */
  private static int getJVMPort(final ConfigParser parser, final int nodeId)
      throws ConfigFileException {
    String[] args;
    if (nodeId == MyriaConstants.MASTER_ID) {
      args = getRequired(parser, "master", nodeId + "").split(":");
    } else {
      args = getRequired(parser, "workers", nodeId + "").split(":");
    }
    if (args.length <= 5 || args[5].equals("")) {
      return 0;
    }
    return Integer.parseInt(args[5]);
  }

  /**
   *
   * @param nodeId the worker/master ID
   * @return the port number
   * @throws ConfigFileException if error occurred when getting the value
   */
  private static int getStatsPort(final ConfigParser parser, final int nodeId)
      throws ConfigFileException {
    String[] args;
    if (nodeId == MyriaConstants.MASTER_ID) {
      args = getRequired(parser, "master", nodeId + "").split(":");
    } else {
      args = getRequired(parser, "workers", nodeId + "").split(":");
    }
    if (args.length <= 4) {
      return 0;
    }
    return Integer.parseInt(args[4]);
  }

  /**
   *
   * @return a list of worker IDs
   * @throws ConfigFileException if error occurred when getting the value
   */
  private static List<Integer> getWorkerIds(final ConfigParser parser) throws ConfigFileException {
    List<Integer> ret = new ArrayList<Integer>();
    try {
      for (Map.Entry<String, String> node : parser.items("workers")) {
        ret.add(Integer.parseInt(node.getKey()));
      }
    } catch (InterpolationMissingOptionException | NoSectionException e) {
      throw new ConfigFileException(e);
    }
    return ret;
  }

  /**
   *
   * @param section the section
   * @param key the key
   * @return the value, must exist
   * @throws ConfigFileException if error occurred when getting the value
   */
  @Nonnull
  private static String getRequired(
      final ConfigParser parser, final String section, final String key)
      throws ConfigFileException {
    try {
      return parser.get(section, key);
    } catch (NoSectionException | NoOptionException | InterpolationException e) {
      throw new ConfigFileException(e);
    }
  }

  /**
   *
   * @param section the section
   * @param key the key
   * @return the value, null if not exist
   */
  @Nullable
  private static String getOptional(
      final ConfigParser parser, final String section, final String key) {
    try {
      return parser.get(section, key);
    } catch (NoSectionException | NoOptionException | InterpolationException e) {
      return null;
    }
  }

  @Nonnull
  private static <T> ConfigurationModule setOptional(
      final ConfigurationModule cm,
      final OptionalParameter<T> param,
      @Nullable final String optionValue) {
    ConfigurationModule conf = cm;
    if (optionValue != null) {
      conf = conf.set(param, optionValue);
    }
    return conf;
  }
}
