package edu.washington.escience.myria.tool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.io.FilenameUtils;
import org.ini4j.ConfigParser;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.coordinator.ConfigFileException;

/** The class to read Myria configuration file, e.g. deployment.cfg. */
public final class MyriaConfiguration extends ConfigParser {

  /** */
  private static final long serialVersionUID = 1L;

  /**
   * load the config file.
   * 
   * @param filename filename.
   * @return parsed mapping from sections to keys to values.
   * @throws ConfigFileException if error occurred when parsing the config file
   * */
  public static MyriaConfiguration loadWithDefaultValues(final String filename) throws ConfigFileException {
    File f = new File(filename);
    if (!f.exists()) {
      throw new RuntimeException("config file " + filename + " doesn't exist!");
    }
    MyriaConfiguration config = new MyriaConfiguration();
    try {
      config.read(f);
      MyriaSystemConfigKeys.addDefaultConfigValues(config);
    } catch (IOException e) {
      throw new ConfigFileException(e);
    }
    return config;
  }

  /**
   * 
   * @return a new Myria configuration.
   */
  public static MyriaConfiguration newConfiguration() {
    return new MyriaConfiguration();
  }

  /**
   * 
   * @param nodeId the node ID
   * @return working directory
   * @throws ConfigFileException if error occurred when getting the value
   */
  public String getPath(final String nodeId) throws ConfigFileException {
    if (!nodeId.equals(MyriaConstants.MASTER_ID + "")) {
      // worker, check if its path is specified
      String[] tmp = getRequired("workers", nodeId).split(":");
      if (tmp.length > 2) {
        return tmp[2];
      }
    }
    return getRequired("deployment", MyriaSystemConfigKeys.DEPLOYMENT_PATH);
  }

  /**
   * 
   * @param nodeId the node ID
   * @return working directory
   * @throws ConfigFileException if error occurred when getting the value
   */
  public String getWorkingDirectory(final String nodeId) throws ConfigFileException {
    return FilenameUtils.concat(getPath(nodeId), getRequired("deployment", MyriaSystemConfigKeys.DESCRIPTION));
  }

  /**
   * 
   * @param workerId the worker ID
   * @return the database name
   * @throws ConfigFileException if error occurred when getting the value
   */
  public String getWorkerDatabaseName(final String workerId) throws ConfigFileException {
    String[] tmp = getRequired("workers", workerId).split(":");
    if (tmp.length > 3) {
      // use the value specified for this worker
      return tmp[3];
    }
    // otherwise the default one
    return getOptional("deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_NAME);
  }

  /**
   * 
   * @param nodeId the worker/master ID
   * @return the hostname
   * @throws ConfigFileException if error occurred when getting the value
   */
  public String getHostname(final String nodeId) throws ConfigFileException {
    if (nodeId.equals(MyriaConstants.MASTER_ID + "")) {
      return getRequired("master", nodeId).split(":")[0];
    } else {
      return getRequired("workers", nodeId).split(":")[0];
    }
  }

  /**
   * 
   * @param nodeId the worker/master ID
   * @return "username@hostname", if username is specified.
   * @throws ConfigFileException if error occurred when getting the value
   */
  public String getHostnameWithUsername(final String nodeId) throws ConfigFileException {
    String hostname = getHostname(nodeId);
    String username = getOptional("deployment", "username");
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
  public String getHostPort(final String nodeId) throws ConfigFileException {
    return getHostname(nodeId) + ":" + getPort(nodeId);
  }

  /**
   * 
   * @param nodeId the worker/master ID
   * @return the port number
   * @throws ConfigFileException if error occurred when getting the value
   */
  public String getPort(final String nodeId) throws ConfigFileException {
    if (nodeId.equals(MyriaConstants.MASTER_ID + "")) {
      return getRequired("master", nodeId).split(":")[1];
    } else {
      return getRequired("workers", nodeId).split(":")[1];
    }
  }

  /**
   * 
   * @return a list of worker IDs
   * @throws ConfigFileException if error occurred when getting the value
   */
  public List<String> getWorkerIds() throws ConfigFileException {
    List<String> ret = new ArrayList<String>();
    try {
      for (Map.Entry<String, String> node : items("workers")) {
        ret.add(node.getKey());
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
  public String getRequired(final String section, final String key) throws ConfigFileException {
    try {
      return get(section, key);
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
  public String getOptional(final String section, final String key) {
    try {
      return get(section, key);
    } catch (NoSectionException | NoOptionException | InterpolationException e) {
      return null;
    }
  }

  /**
   * 
   * @param section the section
   * @param key the key
   * @param value the value, if null don't set
   */
  public void setValue(final String section, final String key, final String value) {
    if (value == null) {
      return;
    }
    try {
      if (!hasSection(section)) {
        addSection(section);
      }
      set(section, key, value);
    } catch (NoSectionException | DuplicateSectionException e) {
      // Should not happen
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the master catalog file
   * @throws ConfigFileException if error occurred parsing the config file
   */
  public String getMasterCatalogFile() throws ConfigFileException {
    String path = getWorkingDirectory(MyriaConstants.MASTER_ID + "");
    path = FilenameUtils.concat(path, "master");
    path = FilenameUtils.concat(path, "master.catalog");
    return path;
  }

  /**
   * 
   * @return a JSON string representation of the connection info
   * @throws ConfigFileException if error occurred parsing the config file
   */
  public String getSelfJsonConnInfo() throws ConfigFileException {
    String id = getRequired("runtime", MyriaSystemConfigKeys.WORKER_IDENTIFIER);
    return ConnectionInfo.toJson(getRequired("deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_SYSTEM),
        getHostname(id), getWorkingDirectory(id), id, getWorkerDatabaseName(id), getOptional("deployment",
            MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_PASSWORD), getOptional("deployment",
            MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_PORT));
  }

  /**
   * 
   * @param config a Myria configuration.
   * @throws ConfigFileException if error occurred
   */
  public void copyRuntimeConfigs(final MyriaConfiguration config) throws ConfigFileException {
    try {
      if (hasSection("runtime")) {
        for (Map.Entry<String, String> entry : items("runtime")) {
          config.setValue("runtime", entry.getKey(), entry.getValue());
        }
      }
    } catch (InterpolationMissingOptionException | NoSectionException e) {
      throw new ConfigFileException(e);
    }
  }
}
