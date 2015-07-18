package edu.washington.escience.myria.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;

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

        System.out.println("Start syncing distribution files to master#" + masterId + " @ " + hostname);
        rsyncFileToRemote("libs", hostname, remotePath, true);
        rsyncFileToRemote("conf", hostname, remotePath);
        rsyncFileToRemote("sqlite4java-392", hostname, remotePath);
        // server needs the config file to create catalogs for new workers
        rsyncFileToRemote(configFileName, hostname, remotePath);
        rsyncFileToRemote("get_logs.py", hostname, remotePath);
        rsyncFileToRemote("myriadeploy.py", hostname, remotePath);
        rsyncFileToRemote("stop_all_by_force.py", hostname, remotePath);
        rsyncFileToRemote("launch_cluster.sh", hostname, remotePath);
        rsyncFileToRemote("using_deployment_utils.sh", hostname, remotePath);
        rsyncFileToRemote("start_master.py", hostname, remotePath);
        rsyncFileToRemote("start_workers.py", hostname, remotePath);
      }
      Map<String, String> workers = config.get("workers");
      for (String workerId : workers.keySet()) {
        workingDir = config.get("paths").get(workerId);
        remotePath = workingDir + "/" + description + "-files";
        String hostname = getHostname(workers.get(workerId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        System.out.println("Start syncing distribution files to worker#" + workerId + " @ " + hostname);
        rsyncFileToRemote("libs", hostname, remotePath, true);
        rsyncFileToRemote("conf", hostname, remotePath);
        rsyncFileToRemote("sqlite4java-392", hostname, remotePath);
      }
    } else if (action.equals("-start_master")) {
      String workingDir = config.get("deployment").get("path");
      String remotePath = workingDir + "/" + description + "-files";
      int restPort = Integer.parseInt(config.get("deployment").get("rest_port"));
      String maxHeapSize = MoreObjects.firstNonNull(config.get("deployment").get("master_heap_size"), "");
      String sslStr = MoreObjects.firstNonNull(config.get("deployment").get("ssl"), "false").toLowerCase();
      boolean ssl = false;
      if (sslStr.equals("true") || sslStr.equals("on") || sslStr.equals("1")) {
        ssl = true;
      }
      Map<String, String> masters = config.get("master");
      for (String masterId : masters.keySet()) {
        String hostname = getHostname(masters.get(masterId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        // server needs the config file to create catalogs for new workers
        rsyncFileToRemote(configFileName, hostname, remotePath);
        startMaster(hostname, workingDir, description, maxHeapSize, restPort, ssl);
      }
    } else if (action.equals("-start_workers")) {
      Map<String, String> heapMapper = config.get("maxheapsizes");
      Map<String, String> workers = config.get("workers");
      boolean debug = config.get("deployment").get("debug_mode").equals("true");
      for (String workerId : workers.keySet()) {
        String hostname = getHostname(workers.get(workerId));
        if (username != null) {
          hostname = username + "@" + hostname;
        }
        String workingDir = config.get("paths").get(workerId);
        int port = getPort(workers.get(workerId));
        String maxHeapSize = heapMapper.get(workerId);
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
    String workerDir = path + "/" + description + "/" + "worker_" + workerId;
    String classpath = "'" + path + "/conf:" + path + "/libs/*'";
    String librarypath = path + "/" + "sqlite4java-392";
    String heapSize = maxHeapSize;
    if (description == null) {
      /* built in system test */
      path = workingDir;
      workerDir = path + "/worker_" + workerId;
      classpath = System.getProperty("java.class.path");
      librarypath = System.getProperty("java.library.path");
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
    builder.append(" 1>" + path + "/worker_" + workerId + "_stdout");
    builder.append(" 2>" + path + "/worker_" + workerId + "_stderr");
    builder.append(" &");
    command[2] = builder.toString();
    System.out.println(workerId + " = " + address);
    System.out.println("");
    startAProcess(command, false);
  }

  /**
   * start a master process on a remote machine.
   * 
   * @param address e.g. beijing.cs.washington.edu
   * @param path the same meaning as path in deployment.cfg
   * @param description the same meaning as name in deployment.cfg
   * @param maxHeapSize the same meaning as max_heap_size in deployment.cfg
   * @param restPort the port number for restlet.
   * @param ssl whether the master uses SSL for the rest server.
   */
  public static void startMaster(final String address, final String path, final String description,
      final String maxHeapSize, final int restPort, final boolean ssl) {
    String workingDir = path + "/" + description + "-files";
    StringBuilder builder = new StringBuilder();
    String[] command = new String[3];
    command[0] = "ssh";
    command[1] = address;
    builder.append(" cd " + workingDir + ";");
    builder.append(" nohup java -cp 'conf:libs/*'");
    builder.append(" -Djava.util.logging.config.file=logging.properties");
    builder.append(" -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=65007");
    builder.append(" -Dlog4j.configuration=log4j.properties");
    builder.append(" -Djava.library.path=sqlite4java-392");
    builder.append(" " + maxHeapSize);
    builder.append(" edu.washington.escience.myria.daemon.MasterDaemon");
    builder.append(" " + workingDir + "/" + description + " " + restPort);
    builder.append(" 0</dev/null");
    builder.append(" 1>master_stdout");
    builder.append(" 2>master_stderr");
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
    ArrayList<String> command = new ArrayList<String>();
    command.add("rsync");
    command.add("-rtLDvz");
    if (useDel) {
      command.add("--del");
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
