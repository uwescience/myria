package edu.washington.escience.myria.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

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
   * @return the hostname and port number of the specified worker.
   */
  @GET
  @Path("/add/worker-{workerId}")
  public String addWorker(@PathParam("workerId") final String sWorkerId) {
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
      server.addWorker(workerId);
    } catch (final RuntimeException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return "New worker " + workerId.toString() + " added";
  }

  /**
   * @param sWorkerId identifier of the worker, input string.
   * @return the hostname and port number of the specified worker.
   */
  @GET
  @Path("/remove/worker-{workerId}")
  public String removeWorker(@PathParam("workerId") final String sWorkerId) {
    Integer workerId;
    try {
      workerId = Integer.parseInt(sWorkerId);
    } catch (final NumberFormatException e) {
      /* Parsing failed, throw a 400 (Bad Request) */
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }
    SocketInfo workerInfo = server.getWorkers().get(workerId);
    if (workerInfo == null) {
      /* Not found, throw a 404 (Not Found) */
      throw new MyriaApiException(Status.NOT_FOUND, sWorkerId);
    }

    if (!server.isWorkerAlive(workerId)) {
      /* Worker already alive, throw a 400 (Bad Request) */
      throw new MyriaApiException(Status.BAD_REQUEST, "Worker not alive");
    }

    /* Yay, worked! */
    server.removeWorker(workerId);
    return "Worker " + workerInfo.toString() + " removed";
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
