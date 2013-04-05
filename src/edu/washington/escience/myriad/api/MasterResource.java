package edu.washington.escience.myriad.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import edu.washington.escience.myriad.daemon.MasterDaemon;

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
   * @return an HTTP 204 (NO CONTENT) response.
   */
  @GET
  @Path("/shutdown")
  public Response shutdown() {
    /* Get the daemon and save it to be used in the stopping. */
    final MasterDaemon daemon =
        (MasterDaemon) MyriaApiUtils.getContextAttributes().get(MyriaApiConstants.MYRIA_MASTER_DAEMON_ATTRIBUTE);

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
    return Response.noContent().build();
  }
}
