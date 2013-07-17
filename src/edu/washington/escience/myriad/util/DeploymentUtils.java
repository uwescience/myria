package edu.washington.escience.myriad.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.tool.MyriaConfigurationReader;

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

    Map<String, HashMap<String, String>> config = READER.load(configFileName);
    String description = config.get("deployment").get("name");
    String username = config.get("deployment").get("username");

    final String action = args[1];
    if (action.equals("-copy_master_catalog")) {
      String workingDir = config.get("deployment").get("path");
      String remotePath = workingDir + "/" + description + "-files" + "/" + description;
      // Although we have only one master now
      HashMap<String, String> masters = config.get("master");
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
      HashMap<String, String> workers = config.get("workers");
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
      HashMap<String, String> masters = config.get("master");
      for (String masterId : masters.keySet()) {
        String hostname = getHostname(masters.get(masterId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        rsyncFileToRemote("libs", hostname, remotePath);
        rsyncFileToRemote("conf", hostname, remotePath);
        rsyncFileToRemote("sqlite4java-282", hostname, remotePath);
        // server needs the config file to create catalogs for new workers
        rsyncFileToRemote(configFileName, hostname, remotePath);
      }
      HashMap<String, String> workers = config.get("workers");
      for (String workerId : workers.keySet()) {
        workingDir = config.get("paths").get(workerId);
        remotePath = workingDir + "/" + description + "-files";
        String hostname = getHostname(workers.get(workerId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        rsyncFileToRemote("libs", hostname, remotePath);
        rsyncFileToRemote("conf", hostname, remotePath);
        rsyncFileToRemote("sqlite4java-282", hostname, remotePath);
      }
    } else if (action.equals("-start_master")) {
      String workingDir = config.get("deployment").get("path");
      String restPort = config.get("deployment").get("rest_port");
      String maxHeapSize = config.get("deployment").get("max_heap_size");
      if (maxHeapSize == null) {
        maxHeapSize = "";
      }
      HashMap<String, String> masters = config.get("master");
      for (String masterId : masters.keySet()) {
        String hostname = getHostname(masters.get(masterId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        startMaster(hostname, workingDir, description, maxHeapSize, restPort);
      }

    } else if (action.equals("-start_workers")) {
      String maxHeapSize = config.get("deployment").get("max_heap_size");
      if (maxHeapSize == null) {
        maxHeapSize = "";
      }
      HashMap<String, String> workers = config.get("workers");
      for (String workerId : workers.keySet()) {
        String hostname = getHostname(workers.get(workerId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        String workingDir = config.get("paths").get(workerId);
        startWorker(hostname, workingDir, description, maxHeapSize, workerId);
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
   */
  public static void startWorker(final String address, final String workingDir, final String description,
      final String maxHeapSize, final String workerId) {
    StringBuilder builder = new StringBuilder();
    builder.append("ssh " + address);
    builder.append(" cd " + workingDir + "/" + description + "-files;");
    builder.append(" nohup java -cp 'libs/*'");
    builder.append(" -Djava.util.logging.config.file=logging.properties");
    builder.append(" -Dlog4j.configuration=log4j.properties");
    builder.append(" -Djava.library.path=sqlite4java-282");
    builder.append(" " + maxHeapSize);
    builder.append(" edu.washington.escience.myriad.parallel.Worker");
    builder.append(" --workingDir " + description + "/worker_" + workerId);
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
      final String maxHeapSize, final String restPort) {
    StringBuilder builder = new StringBuilder();
    builder.append("ssh " + address);
    builder.append(" cd " + workingDir + "/" + description + "-files;");
    builder.append(" nohup java -cp 'libs/*'");
    builder.append(" -Djava.util.logging.config.file=logging.properties");
    builder.append(" -Dlog4j.configuration=log4j.properties");
    builder.append(" -Djava.library.path=sqlite4java-282");
    builder.append(" " + maxHeapSize);
    builder.append(" edu.washington.escience.myriad.daemon.MasterDaemon");
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
        if (request.getResponseCode() == HttpURLConnection.HTTP_OK) {
          break;
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      try {
        Thread.sleep(MyriaConstants.SHORT_WAITING_INTERVAL_10_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      int elapse = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start);
      if (elapse > MyriaConstants.MASTER_START_UP_TIMEOUT_IN_SECOND) {
        throw new RuntimeException("After " + elapse + "s master " + address + " is not alive");
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
    StringBuilder builder = new StringBuilder();
    builder.append("rsync");
    builder.append(" -aLvz");
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
    try {
      new ProcessBuilder().inheritIO().command(cmd.split(" ")).start();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
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
   * util classes are not instantiable.
   * */
  private DeploymentUtils() {
  }
}
