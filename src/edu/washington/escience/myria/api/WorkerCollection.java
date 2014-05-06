package edu.washington.escience.myria.api;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.parallel.SocketInfo;

/**
 * This is the class that handles API calls that return workers.
 * 
 * @author dhalperi
 */
@Path("/workers")
@Produces(MediaType.APPLICATION_JSON)
public final class WorkerCollection {
  /** The Myria server running on the master. */
  @Context
  private Server server;

  /**
   * @return the list of identifiers of workers that are currently alive.
   */
  @GET
  @Path("/alive")
  public Response getAliveWorkers() {
    return Response.ok(server.getAliveWorkers()).cacheControl(MyriaApiUtils.doNotCache()).build();
  }

  /**
   * @param workerId identifier of the worker.
   * @return the hostname and port number of the specified worker.
   */
  @GET
  @Path("/worker-{workerId}")
  public String getWorker(@PathParam("workerId") final String workerId) {
    SocketInfo workerInfo;
    try {
      workerInfo = server.getWorkers().get(Integer.parseInt(workerId));
    } catch (final NumberFormatException e) {
      /* Parsing failed, throw a 400 (Bad Request) */
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }
    if (workerInfo == null) {
      /* Not found, throw a 404 (Not Found) */
      throw new MyriaApiException(Status.NOT_FOUND, workerId);
    }
    /* Yay, worked! */
    return workerInfo.toString();
  }

  /**
   * @param sWorkerId identifier of the worker, input string.
   * @param uriInfo information about the URL of the request.
   * @return the URI of the worker added.
   */
  @POST
  @Path("/start/worker-{workerId}")
  public Response addWorker(@PathParam("workerId") final String sWorkerId, @Context final UriInfo uriInfo) {
    Integer workerId;
    try {
      workerId = Integer.parseInt(sWorkerId);
    } catch (final NumberFormatException e) {
      /* Parsing failed, throw a 400 (Bad Request) */
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }
    if (server.isWorkerAlive(workerId)) {
      /* Worker already alive, throw a 400 (Bad Request) */
      throw new MyriaApiException(Status.BAD_REQUEST, "Worker already alive");
    }
    try {
      server.startWorker(workerId);
    } catch (final RuntimeException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    URI queryUri = uriInfo.getRequestUri();
    return Response.status(Status.ACCEPTED).location(queryUri).build();
  }

  /**
   * @return the set of workers (identifier : host-port string) known by this server.
   */
  @GET
  public Response getWorkers() {
    Map<Integer, SocketInfo> workers;
    workers = server.getWorkers();
    final Map<Integer, String> ret = new HashMap<Integer, String>();
    for (final Entry<Integer, SocketInfo> workerInfo : workers.entrySet()) {
      ret.put(workerInfo.getKey(), workerInfo.getValue().toString());
    }
    /* Don't cache the answer. */
    return Response.ok(ret).cacheControl(MyriaApiUtils.doNotCache()).build();
  }
}
