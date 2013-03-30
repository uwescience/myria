package edu.washington.escience.myriad.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myriad.parallel.SocketInfo;

/**
 * This is the class that handles API calls that return workers.
 * 
 * @author dhalperi
 */
@Path("/workers")
public final class WorkerCollection {
  /**
   * @return the list of identifiers of workers that are currently alive.
   */
  @GET
  @Path("/alive")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<Integer> getAliveWorkers() {
    return MyriaApiUtils.getServer().getAliveWorkers();
  }

  /**
   * @param workerId identifier of the worker.
   * @return the hostname and port number of the specified worker.
   */
  @GET
  @Path("/worker-{workerId}")
  @Produces(MediaType.APPLICATION_JSON)
  public String getWorker(@PathParam("workerId") final String workerId) {
    SocketInfo workerInfo;
    try {
      workerInfo = MyriaApiUtils.getServer().getWorkers().get(Integer.parseInt(workerId));
    } catch (final NumberFormatException e) {
      /* Parsing failed, throw a 400 (Bad Request) */
      throw new WebApplicationException(Response.status(Status.BAD_REQUEST).build());
    }
    if (workerInfo == null) {
      /* Not found, throw a 404 (Not Found) */
      throw new WebApplicationException(Response.status(Status.NOT_FOUND).build());
    }
    /* Yay, worked! */
    return workerInfo.toString();
  }

  /**
   * @return the set of workers (identifier : host-port string) known by this server.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Integer, String> getWorkers() {
    Map<Integer, SocketInfo> workers;
    workers = MyriaApiUtils.getServer().getWorkers();
    final Map<Integer, String> ret = new HashMap<Integer, String>();
    for (final Entry<Integer, SocketInfo> workerInfo : workers.entrySet()) {
      ret.put(workerInfo.getKey(), workerInfo.getValue().toString());
    }
    return ret;
  }
}
