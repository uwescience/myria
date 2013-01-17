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

import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.parallel.SocketInfo;

@Path("/workers")
public final class WorkerCollection {
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Integer, String> getWorkers() {
    Map<Integer, SocketInfo> workers;
    try {
      workers = MasterApiServer.getCatalog().getWorkers();
    } catch (CatalogException e) {
      /* Catalog failed, throw a 500 (Internal Server Error) */
      throw new WebApplicationException(Response.status(Status.INTERNAL_SERVER_ERROR).build());
    }
    Map<Integer, String> ret = new HashMap<Integer, String>();
    for (Entry<Integer, SocketInfo> workerInfo : workers.entrySet()) {
      ret.put(workerInfo.getKey(), workerInfo.getValue().toString());
    }
    return ret;
  }

  @GET
  @Path("/alive")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<Integer> getAliveWorkers() {
    try {
      return MasterApiServer.getCatalog().getAliveWorkers();
    } catch (CatalogException e) {
      /* Catalog failed, throw a 500 (Internal Server Error) */
      throw new WebApplicationException(Response.status(Status.INTERNAL_SERVER_ERROR).build());
    }
  }

  @GET
  @Path("/worker-{workerId}")
  @Produces(MediaType.APPLICATION_JSON)
  public String getWorker(@PathParam("workerId") final String workerId) {
    SocketInfo workerInfo;
    try {
      workerInfo = MasterApiServer.getCatalog().getWorkers().get(Integer.parseInt(workerId));
    } catch (NumberFormatException e) {
      /* Parsing failed, throw a 400 (Bad Request) */
      throw new WebApplicationException(Response.status(Status.BAD_REQUEST).build());
    } catch (CatalogException e) {
      /* Catalog failed, throw a 500 (Internal Server Error) */
      throw new WebApplicationException(Response.status(Status.INTERNAL_SERVER_ERROR).build());
    }
    if (workerInfo == null) {
      /* Not found, throw a 404 (Not Found) */
      throw new WebApplicationException(Response.status(Status.NOT_FOUND).build());
    }
    /* Yay, worked! */
    return workerInfo.toString();
  }
}
