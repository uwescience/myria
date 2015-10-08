package edu.washington.escience.myria.util;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.api.MyriaApiConstants;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.coordinator.ConfigFileException;
import edu.washington.escience.myria.coordinator.ConfigFileGenerator;
import edu.washington.escience.myria.coordinator.MasterCatalog;
import edu.washington.escience.myria.tools.MyriaConfiguration;

/**
 * Deployment util methods.
 * */
public final class DeploymentUtils {

  /** usage. */
  public static final String USAGE =
      "java DeploymentUtils <config_file> <--deploy <optional: --clean-catalog> | --start-master | --start-workers>";
  /** The logger. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DeploymentUtils.class);

  /**
   * entry point.
   * 
   * @param args args.
   * @throws ConfigFileException if error occurred parsing the config file.
   * @throws CatalogException if error occurred generating the catalog.
   * @throws IOException IOException
   * */
  public static void main(final String[] args) throws ConfigFileException, CatalogException, IOException {
    if (args.length < 2) {
      System.out.println(USAGE);
    }

    final String configFileName = args[0];
    MyriaConfiguration config = MyriaConfiguration.loadWithDefaultValues(configFileName);
    final String action = args[1];

    if (action.equals("--deploy")) {
      File tempDeploy = createTempDeployment(configFileName);

      MasterCatalog.create(FilenameUtils.concat(tempDeploy.getAbsolutePath(), "master"));
      boolean cleanCatalog = args.length > 2 && args[2].equals("--clean-catalog");
      deployMaster(tempDeploy.getAbsolutePath(), config, cleanCatalog);

      for (int workerId : config.getWorkerIds()) {
        deployWorker(tempDeploy.getAbsolutePath(), config, workerId);
      }

      FileUtils.deleteDirectory(tempDeploy);
    } else if (action.equals("--start-master")) {
      startMaster(config);
    } else if (action.equals("--start-workers")) {
      for (int workerId : config.getWorkerIds()) {
        startWorker(config, workerId);
      }
    } else {
      System.out.println(USAGE);
    }
  }

  /**
   * 
   * @param config a Myria configuration.
   * @throws ConfigFileException if error occurred parsing config file.
   */
  public static void startMaster(final MyriaConfiguration config) throws ConfigFileException {
    int restPort = Integer.parseInt(config.getRequired("deployment", MyriaSystemConfigKeys.REST_PORT));
    String sslStr =
        MoreObjects.firstNonNull(config.getOptional("deployment", MyriaSystemConfigKeys.SSL), "false").toLowerCase();
    boolean ssl = false;
    if (sslStr.equals("true") || sslStr.equals("on") || sslStr.equals("1")) {
      ssl = true;
    }
    String hostname = config.getHostnameWithUsername(MyriaConstants.MASTER_ID);
    String workingDir = config.getWorkingDirectory(MyriaConstants.MASTER_ID);
    List<String> jvmOptions = new ArrayList<>(config.getJvmOptions());
    String maxHeapSize = config.getOptional("runtime", MyriaSystemConfigKeys.JVM_HEAP_SIZE_MAX_GB);
    if (maxHeapSize != null) {
      jvmOptions.add("-Xmx" + maxHeapSize + "G");
    }
    String minHeapSize = config.getOptional("runtime", MyriaSystemConfigKeys.JVM_HEAP_SIZE_MIN_GB);
    if (minHeapSize != null) {
      jvmOptions.add("-Xms" + minHeapSize + "G");
    }
    jvmOptions.add("-Djava.util.logging.config.file=logging.properties");
    jvmOptions.add("-Dlog4j.configuration=log4j.properties");
    jvmOptions.add("-Djava.library.path=" + workingDir + "/" + "sqlite4java-392");
    String gangliaMasterHost = config.getOptional("deployment", MyriaSystemConfigKeys.GANGLIA_MASTER_HOST);
    if (gangliaMasterHost != null) {
      int gangliaMasterPort =
          Integer.parseInt(config.getRequired("deployment", MyriaSystemConfigKeys.GANGLIA_MASTER_PORT));
      String metricsJarPath = workingDir + "/libs/jmxetric.jar";
      // if the metrics jar file isn't found, the JVM agent will fail to start
      jvmOptions.add("-javaagent:" + metricsJarPath + "=host=" + gangliaMasterHost + ",port=" + gangliaMasterPort
          + ",config=" + workingDir + "/conf/jmxetric.xml,process=MyriaMaster");
    }
    startMaster(hostname, workingDir, restPort, ssl, jvmOptions);
  }

  /**
   * 
   * @param config a Myria configuration.
   * @param workerId worker ID.
   * @throws ConfigFileException if error occurred parsing config file.
   */
  public static void startWorker(final MyriaConfiguration config, final int workerId) throws ConfigFileException {
    String hostname = config.getHostnameWithUsername(workerId);
    String workingDir = config.getWorkingDirectory(workerId);
    int port = config.getPort(workerId);
    boolean debug =
        MoreObjects.firstNonNull(config.getOptional("deployment", MyriaSystemConfigKeys.DEBUG), "false").toLowerCase()
            .equals("true");
    List<String> jvmOptions = new ArrayList<>(config.getJvmOptions());
    if (debug) {
      // required to run in debug mode
      jvmOptions.add("-Dorg.jboss.netty.debug");
      jvmOptions.add("-Xdebug");
      jvmOptions.add("-Xrunjdwp:transport=dt_socket,address=" + (port + 1000) + ",server=y,suspend=n");
    }
    String maxHeapSize = config.getOptional("runtime", MyriaSystemConfigKeys.JVM_HEAP_SIZE_MAX_GB);
    if (maxHeapSize != null) {
      jvmOptions.add("-Xmx" + maxHeapSize + "G");
    }
    String minHeapSize = config.getOptional("runtime", MyriaSystemConfigKeys.JVM_HEAP_SIZE_MIN_GB);
    if (minHeapSize != null) {
      jvmOptions.add("-Xms" + minHeapSize + "G");
    }
    jvmOptions.add("-Djava.util.logging.config.file=logging.properties");
    jvmOptions.add("-Dlog4j.configuration=log4j.properties");
    jvmOptions.add("-Djava.library.path=" + workingDir + "/" + "sqlite4java-392");
    String gangliaMasterHost = config.getOptional("deployment", MyriaSystemConfigKeys.GANGLIA_MASTER_HOST);
    if (gangliaMasterHost != null) {
      int gangliaMasterPort =
          Integer.parseInt(config.getRequired("deployment", MyriaSystemConfigKeys.GANGLIA_MASTER_PORT));
      String metricsJarPath = workingDir + "/libs/jmxetric.jar";
      // if the metrics jar file isn't found, the JVM agent will fail to start
      jvmOptions.add("-javaagent:" + metricsJarPath + "=host=" + gangliaMasterHost + ",port=" + gangliaMasterPort
          + ",config=" + workingDir + "/conf/jmxetric.xml,process=MyriaWorker_" + workerId);
    }
    startWorker(hostname, workingDir, workerId, port, jvmOptions);
  }

  /**
   * 
   * @param configFile the path to a Myria config file.
   * @return the temp deployment directory.
   * @throws IOException I/O exception.
   */
  public static File createTempDeployment(final String configFile) throws IOException {
    final String current = System.getProperty("user.dir");
    File tempDeploy = Files.createTempDirectory("tempdeploy").toFile();
    String tempDeployPath = tempDeploy.getAbsolutePath();
    LOGGER.debug("generating temp deployment at " + tempDeployPath);
    Files.createSymbolicLink(Paths.get(tempDeployPath, "libs"), Paths.get(current, "libs"));
    Files.createSymbolicLink(Paths.get(tempDeployPath, "conf"), Paths.get(current, "conf"));
    Files.createSymbolicLink(Paths.get(tempDeployPath, "sqlite4java-392"), Paths.get(current, "sqlite4java-392"));
    Files.createSymbolicLink(Paths.get(tempDeployPath, MyriaConstants.DEPLOYMENT_CONF_FILE), Paths.get(current,
        configFile));
    return tempDeploy;
  }

  /**
   * 
   * @param localDeployPath source local deployment.
   * @param config a Myria configuration.
   * @param workerId worker ID.
   * @throws ConfigFileException if error occurred parsing config file.
   */
  public static void deployWorker(final String localDeployPath, final MyriaConfiguration config, final int workerId)
      throws ConfigFileException {
    ConfigFileGenerator.makeOneWorkerConfigFile(config, workerId, localDeployPath);
    String workingDir = config.getWorkingDirectory(workerId);
    String hostname = config.getHostnameWithUsername(workerId);
    System.err.println("Start syncing distribution files to worker#" + workerId + " @ " + hostname);
    mkdir(hostname, workingDir);
    List<String> includes = Arrays.asList("workers/" + workerId);
    List<String> excludes =
        Arrays.asList("workers/*", "master", "workers/" + workerId + "/data.db",
            "workers/" + workerId + "/data.db-wal", "workers/" + workerId + "/data.db-shm");
    rsyncFileToRemote(localDeployPath + "/", hostname, workingDir, includes, excludes);
  }

  /**
   * 
   * @param localDeployPath source of local deployment.
   * @param config a Myria configuration.
   * @param cleanCatalog if deploying with a clean master catalog.
   * @throws ConfigFileException if error occurred parsing config file.
   */
  public static void deployMaster(final String localDeployPath, final MyriaConfiguration config,
      final boolean cleanCatalog) throws ConfigFileException {
    String workingDir = config.getWorkingDirectory(MyriaConstants.MASTER_ID);
    String hostname = config.getHostnameWithUsername(MyriaConstants.MASTER_ID);
    String keystoreFile = config.getOptional("deployment", MyriaApiConstants.MYRIA_API_SSL_KEYSTORE);
    System.err.println("Start syncing distribution files to master @ " + hostname);
    mkdir(hostname, workingDir);
    List<String> includes = Arrays.asList("master");
    List<String> excludes = Arrays.asList("workers");
    if (!cleanCatalog) {
      excludes =
          Arrays.asList("workers", "master/master.catalog", "master/master.catalog-wal", "master/master.catalog-shm");
      if (keystoreFile != null) {
        excludes = new ArrayList<>(excludes);
        excludes.add("master/" + keystoreFile);
      }
    }
    rsyncFileToRemote(localDeployPath + "/", hostname, workingDir, includes, excludes);
  }

  /**
   * start a worker process on a remote machine.
   * 
   * @param address e.g. beijing.cs.washington.edu
   * @param workingDir the working directory, path/name in deployment.cfg
   * @param workerId the worker id.
   * @param port the worker port number, need it to infer the port number used in debug mode.
   * @param jvmOptions a list of JVM options.
   */
  private static void startWorker(final String address, final String workingDir, final int workerId, final int port,
      final List<String> jvmOptions) {
    System.err.println(workerId + " = " + address);
    String workerDir = String.format("%s/workers/%d", workingDir, workerId);
    String classpath = String.format("'%s/conf:%s/libs/*'", workingDir, workingDir);
    List<String> args = new ArrayList<>();
    args.add("mkdir -p " + workerDir + ";");
    args.add("cd " + workerDir + ";");
    args.add("nohup java -ea");
    args.add("-cp " + classpath);
    args.addAll(jvmOptions);
    args.add("edu.washington.escience.myria.parallel.Worker");
    args.add("--workingDir " + workerDir);
    args.add("0</dev/null");
    args.add("1>" + workerDir + "/stdout");
    args.add("2>" + workerDir + "/stderr");
    args.add("&");
    String[] command = new String[] { "ssh", address, Joiner.on(" ").join(args) };
    startAProcess(command, false);
  }

  /**
   * start a master process on a remote machine.
   * 
   * @param address e.g. beijing.cs.washington.edu
   * @param workingDir the working directory, path/name in deployment.cfg
   * @param restPort the port number for restlet.
   * @param ssl whether the master uses SSL for the rest server.
   * @param jvmOptions a list of JVM options.
   */
  private static void startMaster(final String address, final String workingDir, final int restPort, final boolean ssl,
      final List<String> jvmOptions) {
    System.err.println(MyriaConstants.MASTER_ID + " = " + address);
    String masterDir = workingDir + "/" + "master";
    String classpath = String.format("'%s/conf:%s/libs/*'", workingDir, workingDir);
    List<String> args = new ArrayList<>();
    args.add("mkdir -p " + masterDir + ";");
    args.add("cd " + masterDir + ";");
    args.add("nohup java -ea");
    args.add("-cp " + classpath);
    args.addAll(jvmOptions);
    args.add("edu.washington.escience.myria.daemon.MasterDaemon");
    args.add(workingDir);
    args.add(restPort + "");
    args.add("0</dev/null");
    args.add("1>" + masterDir + "/stdout");
    args.add("2>" + masterDir + "/stderr");
    args.add("&");
    String[] command = new String[] { "ssh", address, Joiner.on(" ").join(args) };
    startAProcess(command, false);
    String hostname = address;
    if (hostname.indexOf('@') != -1) {
      hostname = address.substring(hostname.indexOf('@') + 1);
    }
    ensureMasterStart(hostname, restPort, ssl);
  }

  /**
   * Ensure that the master is alive. Wait for some time if necessary.
   * 
   * @param hostname the hostname of the master
   * @param restPort the port number of the rest api master
   * @param ssl whether the master is using SSL.
   * */
  public static void ensureMasterStart(final String hostname, final int restPort, final boolean ssl) {

    URL masterAliveUrl;
    try {
      String protocol = "http";
      if (ssl) {
        protocol += 's';
      }
      masterAliveUrl = new URL(protocol + "://" + hostname + ":" + restPort + "/workers/alive");
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    boolean first = true;
    long start = System.currentTimeMillis();
    IOException notAliveException = null;
    while (true) {
      try {
        HttpURLConnection request = (HttpURLConnection) masterAliveUrl.openConnection();
        try {
          if (request.getResponseCode() == HttpURLConnection.HTTP_OK) {
            break;
          }
        } finally {
          if (request != null) {
            request.disconnect();
          }
        }

      } catch (IOException e) {
        if (e instanceof SSLException) {
          /* SSL error means the server is up! */
          break;
        }
        // expected for the first few trials
        if (first) {
          System.err.println("Waiting for the master to be up...");
          first = false;
        }
        notAliveException = e;
      }
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(MyriaConstants.MASTER_START_UP_TIMEOUT_IN_SECOND) / 50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      int elapse = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start);
      if (elapse > MyriaConstants.MASTER_START_UP_TIMEOUT_IN_SECOND) {
        String message = "After " + elapse + "s master " + hostname + ":" + restPort + " is not alive.";
        if (notAliveException != null) {
          message += " Exception: " + notAliveException.getMessage();
        }
        throw new RuntimeException(message);
      }
    }
  }

  /**
   * Call mkdir on a remote machine.
   * 
   * @param address e.g. beijing.cs.washington.edu
   * @param remotePath e.g. /tmp/test
   */
  public static void mkdir(final String address, final String remotePath) {
    startAProcess(new String[] { "ssh", address, "mkdir -p " + remotePath });
  }

  /**
   * Rsync a local deployment to a remote machine.
   * 
   * @param localPath path to the local file that you want to copy from
   * @param address e.g. beijing.cs.washington.edu
   * @param remotePath e.g. /tmp/test
   * @param includes a list of rsync include args
   * @param excludes list of rsync exclude args
   */
  public static void rsyncFileToRemote(final String localPath, final String address, final String remotePath,
      @Nonnull final List<String> includes, @Nonnull final List<String> excludes) {
    ArrayList<String> command = new ArrayList<String>();
    command.add("rsync");
    command.add("-rtLDvz");
    /* Remove old files at dest. */
    command.add("--del");
    for (String inc : includes) {
      command.add("--include=" + inc);
    }
    for (String exc : excludes) {
      command.add("--exclude=" + exc);
    }
    command.add(localPath);
    command.add(address + ":" + remotePath);
    startAProcess(command.toArray(new String[] {}));
  }

  /**
   * Remove a file on a remote machine.
   * 
   * @param address e.g. beijing.cs.washington.edu.
   * @param path the path to the file.
   */
  public static void rmFile(final String address, final String path) {
    startAProcess(new String[] { "ssh", address, "rm -rf " + path });
  }

  /**
   * start a process by ProcessBuilder, wait for the process to finish.
   * 
   * @param cmd cmd[0] is the command name, from cmd[1] are arguments.
   */
  private static void startAProcess(final String[] cmd) {
    startAProcess(cmd, true);
  }

  /**
   * start a process by ProcessBuilder.
   * 
   * @param cmd cmd[0] is the command name, from cmd[1] are arguments.
   * @param waitFor do we wait for the process to finish.
   */
  private static void startAProcess(final String[] cmd, final boolean waitFor) {
    LOGGER.debug(StringUtils.join(cmd, " "));
    try {
      Process p = new ProcessBuilder().inheritIO().command(cmd).start();
      if (waitFor) {
        int ret = p.waitFor();
        if (ret != 0) {
          throw new RuntimeException("Error " + ret + " executing command: " + StringUtils.join(cmd, " "));
        }
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * @param workingDir the working directory.
   * @return the path to the directory holding master catalog
   */
  public static String getPathToMasterDir(final String workingDir) {
    return FilenameUtils.concat(workingDir, "master");
  }

  /**
   * @param workingDir the working directory.
   * @param workerId worker ID.
   * @return the path to the directory holding master catalog
   */
  public static String getPathToWorkerDir(final String workingDir, final int workerId) {
    return Paths.get(workingDir, "workers", workerId + "").toString();
  }

  /**
   * util classes are not instantiable.
   * */
  private DeploymentUtils() {
  }
}
