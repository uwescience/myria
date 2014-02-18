package edu.washington.escience.myria.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.tool.MyriaConfigurationReader;

/**
 * Deployment util methods.
 * */
public final class DeploymentUtils {

  /** usage. */
  public static final String USAGE =
      "java DeploymentUtils <config_file> <-copy_master_catalog | -copy_worker_catalogs | -copy_distribution | -start_master | -start_workers>";
  /** The reader. */
  private static final MyriaConfigurationReader READER = new MyriaConfigurationReader();
  /** The logger. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DeploymentUtils.class);

  /**
   * entry point.
   * 
   * @param args args.
   * @throws IOException if file system error occurs.
   * */
  public static void main(final String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println(USAGE);
    }
    final String configFileName = args[0];

    Map<String, Map<String, String>> config = READER.load(configFileName);
    final String description = config.get("deployment").get("name");
    String username = config.get("deployment").get("username");

    final String action = args[1];
    if (action.equals("-copy_master_catalog")) {
      String workingDir = config.get("deployment").get("path");
      String remotePath = workingDir + "/" + description + "-files" + "/" + description;
      // Although we have only one master now
      Map<String, String> masters = config.get("master");
      for (String masterId : masters.keySet()) {
        String hostname = getHostname(masters.get(masterId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        String localPath = description + "/" + "master.catalog";
        mkdir(hostname, remotePath);
        rsyncFileToRemote(localPath, hostname, remotePath);
        rmFile(hostname, remotePath + "/master.catalog-shm");
        rmFile(hostname, remotePath + "/master.catalog-wal");
      }
    } else if (action.equals("-copy_worker_catalogs")) {
      Map<String, String> workers = config.get("workers");
      for (String workerId : workers.keySet()) {
        String workingDir = config.get("paths").get(workerId);
        String remotePath = workingDir + "/" + description + "-files" + "/" + description;
        String hostname = getHostname(workers.get(workerId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        String localPath = description + "/" + "worker_" + workerId;
        mkdir(hostname, remotePath);
        rsyncFileToRemote(localPath, hostname, remotePath);
        rmFile(hostname, remotePath + "/worker.catalog-shm");
        rmFile(hostname, remotePath + "/worker.catalog-wal");
      }
    } else if (action.equals("-copy_distribution")) {
      String workingDir = config.get("deployment").get("path");
      String remotePath = workingDir + "/" + description + "-files";
      Map<String, String> masters = config.get("master");
      for (String masterId : masters.keySet()) {
        String hostname = getHostname(masters.get(masterId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        rsyncFileToRemote("libs", hostname, remotePath, true);
        rsyncFileToRemote("conf", hostname, remotePath);
        rsyncFileToRemote("sqlite4java-282", hostname, remotePath);
        // server needs the config file to create catalogs for new workers
        rsyncFileToRemote(configFileName, hostname, remotePath);
        rsyncFileToRemote("get_logs.py", hostname, remotePath);
        rsyncFileToRemote("myriadeploy.py", hostname, remotePath);
        rsyncFileToRemote("extract_profiling_data.py", hostname, remotePath);
      }
      Map<String, String> workers = config.get("workers");
      for (String workerId : workers.keySet()) {
        workingDir = config.get("paths").get(workerId);
        remotePath = workingDir + "/" + description + "-files";
        String hostname = getHostname(workers.get(workerId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        rsyncFileToRemote("libs", hostname, remotePath, true);
        rsyncFileToRemote("conf", hostname, remotePath);
        rsyncFileToRemote("sqlite4java-282", hostname, remotePath);
      }
    } else if (action.equals("-start_master")) {
      String workingDir = config.get("deployment").get("path");
      String remotePath = workingDir + "/" + description + "-files";
      int restPort = Integer.parseInt(config.get("deployment").get("rest_port"));
      String maxHeapSize = config.get("deployment").get("max_heap_size");
      if (maxHeapSize == null) {
        maxHeapSize = "";
      }
      Map<String, String> masters = config.get("master");
      for (String masterId : masters.keySet()) {
        String hostname = getHostname(masters.get(masterId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        // server needs the config file to create catalogs for new workers
        rsyncFileToRemote(configFileName, hostname, remotePath);
        startMaster(hostname, workingDir, description, maxHeapSize, restPort);
      }

    } else if (action.equals("-start_workers")) {
      String maxHeapSize = config.get("deployment").get("max_heap_size");
      if (maxHeapSize == null) {
        maxHeapSize = "";
      }
      Map<String, String> workers = config.get("workers");
      boolean debug = config.get("deployment").get("debug_mode").equals("true");
      for (String workerId : workers.keySet()) {
        String hostname = getHostname(workers.get(workerId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        String workingDir = config.get("paths").get(workerId);
        int port = getPort(workers.get(workerId));
        startWorker(hostname, workingDir, description, maxHeapSize, workerId, port, debug);
      }
    } else {
      System.out.println(USAGE);
    }
  }

  /**
   * start a worker process on a remote machine.
   * 
   * @param address e.g. beijing.cs.washington.edu
   * @param workingDir the same meaning as path in deployment.cfg
   * @param description the same meaning as name in deployment.cfg
   * @param maxHeapSize the same meaning as max_heap_size in deployment.cfg
   * @param workerId the worker id.
   * @param port the worker port number, need it to infer the port number used in debug mode.
   * @param debug if launch the worker in debug mode.
   */
  public static void startWorker(final String address, final String workingDir, final String description,
      final String maxHeapSize, final String workerId, final int port, final boolean debug) {
    StringBuilder builder = new StringBuilder();
    String path = workingDir + "/" + description + "-files";
    String workerDir = description + "/" + "worker_" + workerId;
    String classpath = "'conf:libs/*'";
    String librarypath = "sqlite4java-282";
    String heapSize;
    if (maxHeapSize != null) {
      heapSize = maxHeapSize;
    } else {
      heapSize = "";
    }

    if (description == null) {
      /* built in system test */
      path = workingDir;
      workerDir = "worker_" + workerId;
      classpath = System.getProperty("java.class.path");
      librarypath = System.getProperty("java.library.path");
      heapSize = "";
    }

    builder.append("ssh " + address);
    builder.append(" cd " + path + ";");
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
    builder.append(" 1>worker_" + workerId + "_stdout");
    builder.append(" 2>worker_" + workerId + "_stderr");
    builder.append(" &");
    System.out.println(workerId + " = " + address);
    startAProcess(builder.toString());
  }

  /**
   * start a master process on a remote machine.
   * 
   * @param address e.g. beijing.cs.washington.edu
   * @param workingDir the same meaning as path in deployment.cfg
   * @param description the same meaning as name in deployment.cfg
   * @param maxHeapSize the same meaning as max_heap_size in deployment.cfg
   * @param restPort the port number for restlet.
   */
  public static void startMaster(final String address, final String workingDir, final String description,
      final String maxHeapSize, final int restPort) {
    StringBuilder builder = new StringBuilder();
    builder.append("ssh " + address);
    builder.append(" cd " + workingDir + "/" + description + "-files;");
    builder.append(" nohup java -cp 'conf:libs/*'");
    builder.append(" -Djava.util.logging.config.file=logging.properties");
    builder.append(" -Dlog4j.configuration=log4j.properties");
    builder.append(" -Djava.library.path=sqlite4java-282");
    builder.append(" " + maxHeapSize);
    builder.append(" edu.washington.escience.myria.daemon.MasterDaemon");
    builder.append(" " + description + " " + restPort);
    builder.append(" 0</dev/null");
    builder.append(" 1>master_stdout");
    builder.append(" 2>master_stderr");
    builder.append(" &");
    System.out.println(address);
    startAProcess(builder.toString());
    String hostname = address;
    if (hostname.indexOf('@') != -1) {
      hostname = address.substring(hostname.indexOf('@') + 1);
    }
    ensureMasterStart(hostname, restPort);

  }

  /**
   * Ensure that the master is alive. Wait for some time if necessary.
   * 
   * @param hostname the hostname of the master
   * @param restPort the port number of the rest api master
   * */
  public static void ensureMasterStart(final String hostname, final int restPort) {

    URL masterAliveUrl;
    try {
      masterAliveUrl = new URL("http://" + hostname + ":" + restPort + "/workers/alive");
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    long start = System.currentTimeMillis();
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
        // expected for the first few trials
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(e.toString());
        }
      }
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(MyriaConstants.MASTER_START_UP_TIMEOUT_IN_SECOND) / 10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      int elapse = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start);
      if (elapse > MyriaConstants.MASTER_START_UP_TIMEOUT_IN_SECOND) {
        throw new RuntimeException("After " + elapse + "s master " + hostname + ":" + restPort + " is not alive");
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
    StringBuilder builder = new StringBuilder();
    builder.append("ssh");
    builder.append(" " + address);
    builder.append(" mkdir -p");
    builder.append(" " + remotePath);
    startAProcess(builder.toString());
  }

  /**
   * Copy a local file to a location on a remote machine, using rsync.
   * 
   * @param localPath path to the local file that you want to copy from
   * @param address e.g. beijing.cs.washington.edu
   * @param remotePath e.g. /tmp/test
   */
  public static void rsyncFileToRemote(final String localPath, final String address, final String remotePath) {
    rsyncFileToRemote(localPath, address, remotePath, false);
  }

  /**
   * Copy a local file to a location on a remote machine, using rsync.
   * 
   * @param localPath path to the local file that you want to copy from
   * @param address e.g. beijing.cs.washington.edu
   * @param remotePath e.g. /tmp/test
   * @param useDel if use --del option
   */
  public static void rsyncFileToRemote(final String localPath, final String address, final String remotePath,
      final boolean useDel) {
    StringBuilder builder = new StringBuilder();
    builder.append("rsync");
    builder.append(" -aLvz");
    if (useDel) {
      builder.append(" --del");
    }
    builder.append(" " + localPath);
    builder.append(" " + address + ":" + remotePath);
    startAProcess(builder.toString());
  }

  /**
   * Remove a file on a remote machine.
   * 
   * @param address e.g. beijing.cs.washington.edu.
   * @param path the path to the file.
   */
  public static void rmFile(final String address, final String path) {
    StringBuilder builder = new StringBuilder();
    builder.append("ssh");
    builder.append(" " + address);
    builder.append(" rm -rf");
    builder.append(" " + path);
    startAProcess(builder.toString());
  }

  /**
   * start a process by ProcessBuilder.
   * 
   * @param cmd the command.
   */
  private static void startAProcess(final String cmd) {
    LOGGER.debug(cmd);
    int ret;
    try {
      ret = new ProcessBuilder().inheritIO().command(cmd.split(" ")).start().waitFor();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    if (ret != 0) {
      throw new RuntimeException("Error " + ret + " executing command: " + cmd);
    }
  }

  /**
   * Helper function to get the hostname from hostname:port.
   * 
   * @param s the string hostname:port
   * @return the hostname.
   * */
  private static String getHostname(final String s) {
    return s.split(":")[0];
  }

  /**
   * Helper function to get the port number from hostname:port.
   * 
   * @param s the string hostname:port
   * @return the port number.
   * */
  private static int getPort(final String s) {
    return Integer.parseInt(s.split(":")[1]);
  }

  /**
   * util classes are not instantiable.
   * */
  private DeploymentUtils() {
  }
}
