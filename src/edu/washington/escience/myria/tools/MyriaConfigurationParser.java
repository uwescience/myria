package edu.washington.escience.myria.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.io.FilenameUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationModule;
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
    return cm.build();
  }

  private static ConfigurationModule setGlobalConfVariables(final ConfigParser parser,
      final ConfigurationModule cm) throws ConfigFileException {
    return cm
        .set(MyriaGlobalConfigurationModule.INSTANCE_NAME,
            getRequired(parser, "deployment", MyriaSystemConfigKeys.DESCRIPTION))
        .set(MyriaGlobalConfigurationModule.DEFAULT_INSTANCE_PATH,
            getPath(parser, MyriaConstants.MASTER_ID))
        .set(MyriaGlobalConfigurationModule.STORAGE_DBMS,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_SYSTEM))
        .set(MyriaGlobalConfigurationModule.DEFAULT_STORAGE_DB_NAME,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_NAME))
        .set(MyriaGlobalConfigurationModule.DEFAULT_STORAGE_DB_USERNAME,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.USERNAME))
        .set(
            MyriaGlobalConfigurationModule.DEFAULT_STORAGE_DB_PASSWORD,
            getOptional(parser, "deployment",
                MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_PASSWORD))
        .set(MyriaGlobalConfigurationModule.DEFAULT_STORAGE_DB_PORT,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_PORT))
        .set(MyriaGlobalConfigurationModule.REST_API_PORT,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.REST_PORT))
        .set(MyriaGlobalConfigurationModule.API_ADMIN_PASSWORD,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.ADMIN_PASSWORD))
        .set(MyriaGlobalConfigurationModule.USE_SSL,
            Boolean.parseBoolean(getOptional(parser, "deployment", MyriaSystemConfigKeys.SSL)))
        .set(MyriaGlobalConfigurationModule.SSL_KEYSTORE_PATH,
            getOptional(parser, "deployment", MyriaApiConstants.MYRIA_API_SSL_KEYSTORE))
        .set(MyriaGlobalConfigurationModule.SSL_KEYSTORE_PASSWORD,
            getOptional(parser, "deployment", MyriaApiConstants.MYRIA_API_SSL_KEYSTORE_PASSWORD))
        .set(MyriaGlobalConfigurationModule.ENABLE_DEBUG,
            Boolean.parseBoolean(getOptional(parser, "deployment", MyriaSystemConfigKeys.DEBUG)))
        .set(MyriaGlobalConfigurationModule.NUMBER_VCORES,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.NUMBER_VCORES))
        .set(MyriaGlobalConfigurationModule.MEMORY_QUOTA_GB,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.MEMORY_QUOTA_GB))
        .set(MyriaGlobalConfigurationModule.JVM_HEAP_SIZE_MIN_GB,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.JVM_HEAP_SIZE_MIN_GB))
        .set(MyriaGlobalConfigurationModule.JVM_HEAP_SIZE_MAX_GB,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.JVM_HEAP_SIZE_MAX_GB))
        .set(MyriaGlobalConfigurationModule.JVM_OPTIONS,
            getOptional(parser, "runtime", MyriaSystemConfigKeys.JVM_OPTIONS))
        .set(
            MyriaGlobalConfigurationModule.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES,
            getOptional(parser, "deployment",
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES))
        .set(
            MyriaGlobalConfigurationModule.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES,
            getOptional(parser, "deployment",
                MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES))
        .set(MyriaGlobalConfigurationModule.OPERATOR_INPUT_BUFFER_CAPACITY,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY))
        .set(
            MyriaGlobalConfigurationModule.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER,
            getOptional(parser, "deployment",
                MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER))
        .set(MyriaGlobalConfigurationModule.TCP_CONNECTION_TIMEOUT_MILLIS,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.TCP_CONNECTION_TIMEOUT_MILLIS))
        .set(MyriaGlobalConfigurationModule.TCP_SEND_BUFFER_SIZE_BYTES,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES))
        .set(MyriaGlobalConfigurationModule.TCP_RECEIVE_BUFFER_SIZE_BYTES,
            getOptional(parser, "deployment", MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES))
        .set(MyriaGlobalConfigurationModule.MASTER_HOST,
            getHostname(parser, MyriaConstants.MASTER_ID))
        .set(MyriaGlobalConfigurationModule.MASTER_RPC_PORT,
            getPort(parser, MyriaConstants.MASTER_ID));
  }

  private static ConfigurationModule setWorkerConfs(final ConfigParser parser,
      final ConfigurationModule cm) throws ConfigFileException {
    ConfigurationModule conf = cm;
    for (Integer workerId : getWorkerIds(parser)) {
      Configuration workerConfig =
          MyriaWorkerConfigurationModule.CONF
              .set(MyriaWorkerConfigurationModule.WORKER_ID, workerId + "")
              .set(MyriaWorkerConfigurationModule.WORKER_HOST, getHostname(parser, workerId))
              .set(MyriaWorkerConfigurationModule.WORKER_PORT, getPort(parser, workerId))
              .set(MyriaWorkerConfigurationModule.WORKER_STORAGE_DB_NAME,
                  getWorkerDatabaseName(parser, workerId))
              .set(MyriaWorkerConfigurationModule.WORKER_FILESYSTEM_PATH, getPath(parser, workerId))
              .build();
      String serializedWorkerConfig = new AvroConfigurationSerializer().toString(workerConfig);
      conf = conf.set(MyriaGlobalConfigurationModule.WORKER_CONF, serializedWorkerConfig);
    }
    return conf;
  }

  /**
   * 
   * @param nodeId the node ID
   * @return working directory
   * @throws ConfigFileException if error occurred when getting the value
   */
  public static String getPath(final ConfigParser parser, final int nodeId)
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
   * @param nodeId the node ID
   * @return working directory
   * @throws ConfigFileException if error occurred when getting the value
   */
  public static String getWorkingDirectory(final ConfigParser parser, final int nodeId)
      throws ConfigFileException {
    return FilenameUtils.concat(getPath(parser, nodeId),
        getRequired(parser, "deployment", MyriaSystemConfigKeys.DESCRIPTION));
  }

  /**
   * 
   * @param workerId the worker ID
   * @return the database name
   * @throws ConfigFileException if error occurred when getting the value
   */
  public static String getWorkerDatabaseName(final ConfigParser parser, final int workerId)
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
  public static String getHostname(final ConfigParser parser, final int nodeId)
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
   * @return "username@hostname", if username is specified.
   * @throws ConfigFileException if error occurred when getting the value
   */
  public static String getHostnameWithUsername(final ConfigParser parser, final int nodeId)
      throws ConfigFileException {
    String hostname = getHostname(parser, nodeId);
    String username = getOptional(parser, "deployment", "username");
    if (username != null) {
      hostname = username + "@" + hostname;
    }
    return hostname;
  }

  /**
   * 
   * @param nodeId the worker/master ID
   * @return a string in the format of hostname:port
   * @throws ConfigFileException if error occurred when getting the value
   */
  public static String getHostPort(final ConfigParser parser, final int nodeId)
      throws ConfigFileException {
    return getHostname(parser, nodeId) + ":" + getPort(parser, nodeId);
  }

  /**
   * 
   * @param nodeId the worker/master ID
   * @return the port number
   * @throws ConfigFileException if error occurred when getting the value
   */
  public static int getPort(final ConfigParser parser, final int nodeId) throws ConfigFileException {
    if (nodeId == MyriaConstants.MASTER_ID) {
      return Integer.parseInt(getRequired(parser, "master", nodeId + "").split(":")[1]);
    } else {
      return Integer.parseInt(getRequired(parser, "workers", nodeId + "").split(":")[1]);
    }
  }

  /**
   * 
   * @return a list of worker IDs
   * @throws ConfigFileException if error occurred when getting the value
   */
  public static List<Integer> getWorkerIds(final ConfigParser parser) throws ConfigFileException {
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
  public static String getRequired(final ConfigParser parser, final String section, final String key)
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
  public static String getOptional(final ConfigParser parser, final String section, final String key) {
    try {
      return parser.get(section, key);
    } catch (NoSectionException | NoOptionException | InterpolationException e) {
      return null;
    }
  }

  /**
   * @return the master catalog file
   * @throws ConfigFileException if error occurred parsing the config file
   */
  public static String getMasterCatalogFile(final ConfigParser parser) throws ConfigFileException {
    String path = getWorkingDirectory(parser, MyriaConstants.MASTER_ID);
    path = FilenameUtils.concat(path, "master");
    path = FilenameUtils.concat(path, "master.catalog");
    return path;
  }

  /**
   * 
   * @return JVM options
   */
  public static List<String> getJvmOptions(final ConfigParser parser) {
    String options = getOptional(parser, "runtime", MyriaSystemConfigKeys.JVM_OPTIONS);
    if (options != null) {
      return Arrays.asList(options.split(" "));
    }
    return Collections.emptyList();
  }
}
