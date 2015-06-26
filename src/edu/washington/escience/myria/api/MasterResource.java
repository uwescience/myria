package edu.washington.escience.myria.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.commons.io.FilenameUtils;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.api.MasterApplication.ADMIN;
import edu.washington.escience.myria.api.encoding.VersionEncoding;
import edu.washington.escience.myria.coordinator.ConfigFileException;
import edu.washington.escience.myria.daemon.MasterDaemon;
import edu.washington.escience.myria.parallel.Server;

/**
 * This is the class that handles API calls that return workers.
 * 
 * @author jwang
 */
@Path("/server")
public final class MasterResource {
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
    /* A thread to stop the daemon after this request finishes. */
    Thread shutdownThread = new Thread("MasterResource-Shutdown") {
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
