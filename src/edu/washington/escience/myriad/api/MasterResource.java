package edu.washington.escience.myriad.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * This is the class that handles API calls that return workers.
 * 
 * @author jwang
 */
@Path("/server")
public final class MasterResource {
  /**
   * Shutdown the server.
   */
  @GET
  @Path("/shutdown")
  public void shutdown() {
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
    shutdownThread.setName("Shutdown thread");
    shutdownThread.start();
  }
}
