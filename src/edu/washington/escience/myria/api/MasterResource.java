package edu.washington.escience.myria.api;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.api.MasterApplication.ADMIN;
import edu.washington.escience.myria.api.encoding.VersionEncoding;
import edu.washington.escience.myria.coordinator.ConfigFileException;
import edu.washington.escience.myria.daemon.MasterDaemon;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.DeploymentUtils;

/**
 * This is the class that handles API calls that return workers.
 *
 * @author jwang
 */
@Path("/server")
public final class MasterResource {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterResource.class);

  /** How long the thread should sleep before shutting down the daemon. This is a HACK! TODO */
  private static final int SLEEP_BEFORE_SHUTDOWN_MS = 100;

  /**
   * Shutdown the server.
   *
   * @param daemon the Myria {@link MasterDaemon} to be shutdown.
   * @return a success message.
   */
  @GET
  @Path("/shutdown")
  @ADMIN
  public Response shutdown(@Context final MasterDaemon daemon) {
    doShutdown(daemon);
    return Response.ok("Success").build();
  }

  private static void doShutdown(final MasterDaemon daemon) {
    /* A thread to stop the daemon after this request finishes. */
    Thread shutdownThread =
        new Thread("MasterResource-Shutdown") {
          @Override
          public void run() {
            try {
              Thread.sleep(SLEEP_BEFORE_SHUTDOWN_MS);
              daemon.stop();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        };

    /* Start the thread, then return an empty success response. */
    shutdownThread.start();
  }

  private static int getProcessPid() {
    // HACK: in the Oracle JVM, we can get the current process's PID as the first component of the value returned by
    // ManagementFactory.getRuntimeMXBean().getName().
    // This method is documented "Returns the name representing the running Java virtual machine."
    // Example: "742912@localhost"
    String pidStr = ManagementFactory.getRuntimeMXBean().getName();
    if (pidStr.contains("@")) {
      return Integer.parseInt(pidStr.split("@")[0]);
    } else {
      throw new RuntimeException("Cannot find pid from string: " + pidStr);
    }
  }

  // NB: this is superseded by Process.isAlive() in Java 8
  private static boolean isAlive(final Process p) {
    try {
      p.exitValue();
      return false;
    } catch (IllegalThreadStateException e) {
      return true;
    }
  }

  /**
   * Restart the server.
   *
   * @param server the Myria {@link Server} to be restarted.
   * @return a success message.
   * @throws ConfigFileException
   */
  @GET
  @Path("/restart")
  @ADMIN
  public Response restart(@Context final MasterDaemon daemon) throws ConfigFileException {
    String workingDir =
        daemon.getClusterMaster().getConfig().getWorkingDirectory(MyriaConstants.MASTER_ID);
    String deploymentFile = FilenameUtils.concat(workingDir, MyriaConstants.DEPLOYMENT_CONF_FILE);
    int pid = getProcessPid();
    String classpath = String.format("'%s/conf:%s/libs/*'", workingDir, workingDir);
    String command =
        String.format(
            "java -cp %s edu.washington.escience.myria.util.MyriaLauncher %s %s",
            classpath,
            deploymentFile,
            Integer.toString(pid));
    List<String> args = new ArrayList<>();
    // HACK: If we launch the java process directly, then for some reason the child process exits as soon as the parent
    // exits (even if we don't inherit open file descriptors).
    args.add("/bin/sh");
    args.add("-c");
    args.add(command);
    LOGGER.debug("Command line for MyriaLauncher:\n{}", Joiner.on(" ").join(args));
    Process p = DeploymentUtils.startAProcess(args.toArray(new String[args.size()]), false, false);
    doShutdown(daemon);
    LOGGER.debug("MyriaLauncher alive: {}", isAlive(p));
    return Response.ok("Success").build();
  }

  /**
   * Get the version information of Myria at build time.
   *
   * @return a {@link VersionEncoding}.
   */
  @GET
  @Produces(MyriaApiConstants.JSON_UTF_8)
  public Response getVersion() {
    return Response.ok(new VersionEncoding()).build();
  }

  /**
   * Get the path to the deployment.cfg file.
   *
   * @param server the Myria {@link Server}.
   * @return the file path.
   */
  @GET
  @Path("/deployment_cfg")
  @ADMIN
  public Response getDeploymentCfg(@Context final Server server) {
    try {
      String workingDir = server.getConfig().getWorkingDirectory(MyriaConstants.MASTER_ID);
      String deploymentFile = FilenameUtils.concat(workingDir, MyriaConstants.DEPLOYMENT_CONF_FILE);
      return Response.ok(deploymentFile).build();
    } catch (ConfigFileException e) {
      return Response.ok(e.getMessage()).build();
    }
  }
}
