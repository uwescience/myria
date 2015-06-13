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

import javax.net.ssl.SSLException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.coordinator.ConfigFileException;
import edu.washington.escience.myria.coordinator.ConfigFileGenerator;
import edu.washington.escience.myria.coordinator.MasterCatalog;
import edu.washington.escience.myria.tool.MyriaConfiguration;

/**
 * Deployment util methods.
 * */
public final class DeploymentUtils {

  /** usage. */
  public static final String USAGE =
      "java DeploymentUtils <config_file> <-deploy <optional: -fresh_catalog> | -start_master | -start_workers>";
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

    if (action.equals("-deploy")) {
      File tempDeploy = createTempDeployment(configFileName);

      MasterCatalog.create(FilenameUtils.concat(tempDeploy.getAbsolutePath(), "master"));
      boolean freshCatalog = args.length > 2 && args[2].equals("-fresh_catalog");
      deployMaster(tempDeploy.getAbsolutePath(), config, freshCatalog);

      for (String workerId : config.getWorkerIds()) {
        deployWorker(tempDeploy.getAbsolutePath(), config, workerId);
      }

      FileUtils.deleteDirectory(tempDeploy);
    } else if (action.equals("-start_master")) {
      startMaster(config);
    } else if (action.equals("-start_workers")) {
      for (String workerId : config.getWorkerIds()) {
        startAWorker(config, workerId);
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
    String maxHeapSize = config.getOptional("runtime", MyriaSystemConfigKeys.MAX_HEAP_SIZE);
    String sslStr =
        MoreObjects.firstNonNull(config.getOptional("deployment", MyriaSystemConfigKeys.SSL), "false").toLowerCase();
    boolean ssl = false;
    if (sslStr.equals("true") || sslStr.equals("on") || sslStr.equals("1")) {
      ssl = true;
    }
    String hostname = config.getHostnameWithUsername(MyriaConstants.MASTER_ID + "");
    String workingDir = config.getWorkingDirectory(MyriaConstants.MASTER_ID + "");
    startMaster(hostname, workingDir, maxHeapSize, restPort, ssl);
  }

  /**
   * 
   * @param config a Myria configuration.
   * @param workerId worker ID.
   * @throws ConfigFileException if error occurred parsing config file.
   */
  public static void startAWorker(final MyriaConfiguration config, final String workerId) throws ConfigFileException {
    String maxHeapSize = config.getOptional("runtime", MyriaSystemConfigKeys.MAX_HEAP_SIZE);
    boolean debug =
        MoreObjects.firstNonNull(config.getOptional("deployment", MyriaSystemConfigKeys.DEBUG), "false").toLowerCase()
            .equals("true");
    String hostname = config.getHostnameWithUsername(workerId);
    String workingDir = config.getWorkingDirectory(workerId);
    int port = Integer.parseInt(config.getPort(workerId));
    startWorker(hostname, workingDir, maxHeapSize, workerId, port, debug);
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
    System.out.println("generating temp deployment at " + tempDeployPath);
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
  public static void deployWorker(final String localDeployPath, final MyriaConfiguration config, final String workerId)
      throws ConfigFileException {
    ConfigFileGenerator.makeOneWorkerConfigFile(config, workerId, localDeployPath);
    String workingDir = config.getWorkingDirectory(workerId);
    String hostname = config.getHostnameWithUsername(workerId);
    System.out.println("Start syncing distribution files to worker#" + workerId + " @ " + hostname);
    mkdir(hostname, workingDir);
    List<String> includes = Arrays.asList("worker_" + workerId);
    List<String> excludes = Arrays.asList("worker_*", "master");
    rsyncFileToRemote(localDeployPath + "/", hostname, workingDir, includes, excludes);
  }

  /**
   * 
   * @param localDeployPath source local deployment.
   * @param config a Myria configuration.
   * @param freshCatalog if deploy with an empty master catalog.
   * @throws ConfigFileException if error occurred parsing config file.
   */
  public static void deployMaster(final String localDeployPath, final MyriaConfiguration config,
      final boolean freshCatalog) throws ConfigFileException {
    String workingDir = config.getWorkingDirectory(MyriaConstants.MASTER_ID + "");
    String hostname = config.getHostnameWithUsername(MyriaConstants.MASTER_ID + "");
    System.out.println("Start syncing distribution files to master @ " + hostname);
    mkdir(hostname, workingDir);
    List<String> includes = Arrays.asList("master");
    List<String> excludes = Arrays.asList("worker_*");
    if (!freshCatalog) {
      excludes = Arrays.asList("worker_*", "master/master.catalog");
    }
    rsyncFileToRemote(localDeployPath + "/", hostname, workingDir, includes, excludes);
  }

  /**
   * start a worker process on a remote machine.
   * 
   * @param address e.g. beijing.cs.washington.edu
   * @param workingDir the working directory, path/name in deployment.cfg
   * @param maxHeapSize the same meaning as max_heap_size in deployment.cfg
   * @param workerId the worker id.
   * @param port the worker port number, need it to infer the port number used in debug mode.
   * @param debug if launch the worker in debug mode.
   */
  private static void startWorker(final String address, final String workingDir, final String maxHeapSize,
      final String workerId, final int port, final boolean debug) {
    StringBuilder builder = new StringBuilder();
    String workerDir = workingDir + "/" + "worker_" + workerId;
    String classpath = "'" + workingDir + "/conf:" + workingDir + "/libs/*'";
    String librarypath = workingDir + "/" + "sqlite4java-392";
    String heapSize = maxHeapSize;
    if (heapSize == null) {
      heapSize = "";
    }
    String[] command = new String[3];
    command[0] = "ssh";
    command[1] = address;

    builder.append("mkdir -p " + workerDir + ";");
    builder.append(" cd " + workerDir + ";");
    builder.append(" nohup java -ea");
    builder.append(" -cp " + classpath);
    builder.append(" -Djava.util.logging.config.file=logging.properties");
    builder.append(" -Dlog4j.configuration=log4j.properties");
    builder.append(" -Djava.library.path=" + librarypath);
    if (debug) {
      // required to run in debug mode
      builder.append(" -Dorg.jboss.netty.debug");
      builder.append(" -Xdebug");
      builder.append(" -Xrunjdwp:transport=dt_socket,address=" + (port + 1000) + ",server=y,suspend=n");
    }
    builder.append(" " + heapSize);
    builder.append(" edu.washington.escience.myria.parallel.Worker");
    builder.append(" --workingDir " + workerDir);
    builder.append(" 0</dev/null");
    builder.append(" 1>" + workerDir + "/worker_" + workerId + "_stdout");
    builder.append(" 2>" + workerDir + "/worker_" + workerId + "_stderr");
    builder.append(" &");
    command[2] = builder.toString();
    System.out.println(workerId + " = " + address);
    startAProcess(command, false);
  }

  /**
   * start a master process on a remote machine.
   * 
   * @param address e.g. beijing.cs.washington.edu
   * @param workingDir the working directory, path/name in deployment.cfg
   * @param maxHeapSize the same meaning as max_heap_size in deployment.cfg
   * @param restPort the port number for restlet.
   * @param ssl whether the master uses SSL for the rest server.
   */
  private static void startMaster(final String address, final String workingDir, final String maxHeapSize,
      final int restPort, final boolean ssl) {
    String masterDir = workingDir + "/" + "master";
    StringBuilder builder = new StringBuilder();
    String heapSize = maxHeapSize;
    if (heapSize == null) {
      heapSize = "";
    }
    String[] command = new String[3];
    command[0] = "ssh";
    command[1] = address;
    builder.append(" cd " + workingDir + ";");
    builder.append(" nohup java -cp 'conf:libs/*'");
    builder.append(" -Djava.util.logging.config.file=logging.properties");
    builder.append(" -Dlog4j.configuration=log4j.properties");
    builder.append(" -Djava.library.path=sqlite4java-392");
    builder.append(" " + heapSize);
    builder.append(" edu.washington.escience.myria.daemon.MasterDaemon");
    builder.append(" " + workingDir + " " + restPort);
    builder.append(" 0</dev/null");
    builder.append(" 1>" + masterDir + "/master_stdout");
    builder.append(" 2>" + masterDir + "/master_stderr");
    builder.append(" &");
    command[2] = builder.toString();
    System.out.println(address);
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
          System.out.println("Waiting for the master to be up...");
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
   * Copy a local file to a location on a remote machine, using rsync.
   * 
   * @param localPath path to the local file that you want to copy from
   * @param address e.g. beijing.cs.washington.edu
   * @param remotePath e.g. /tmp/test
   */
  public static void rsyncFileToRemote(final String localPath, final String address, final String remotePath) {
    rsyncFileToRemote(localPath, address, remotePath, null, null);
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
      final List<String> includes, final List<String> excludes) {
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
    return FilenameUtils.concat(workingDir, "worker_" + workerId);
  }

  /**
   * util classes are not instantiable.
   * */
  private DeploymentUtils() {
  }
}
