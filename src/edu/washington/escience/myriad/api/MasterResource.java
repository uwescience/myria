package edu.washington.escience.myriad.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

/**
 * This is the class that handles API calls that return workers.
 * 
 * @author jwang
 */
@Path("/server")
public final class MasterResource {
  /**
   * Shutdown the server.
   * 
   * @return an HTTP 204 (NO CONTENT) response.
   */
  @GET
  @Path("/shutdown")
  public Response shutdown() {
    MasterApiServer.getMyriaServer().shutdown();
    Thread shutdownThread = new Thread() {
      @Override
      public void run() {
        try {
          MasterApiServer.INSTANCE.stop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    shutdownThread.setName("MasterResource-Shutdown");
    shutdownThread.start();
    return Response.noContent().build();
  }
}
