package edu.washington.escience.myria.api;

import java.nio.file.Paths;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import edu.washington.escience.myria.parallel.Worker;

/**
 * This is the class that handles API calls that return workers.
 *
 */
@Path("/")
@Produces(MyriaApiConstants.JSON_UTF_8)
public final class WorkerResource {
  /** The Myria server running on the worker. */
  @Context private Worker worker;

  /**
   * @param qid query id.
   * @return the stats of all hash tables of the given query.
   */
  @GET
  @Path("/hashtable-query-{qid:\\d+}")
  public Response getHashTableStats(@PathParam("qid") final long qid) {
    return Response.ok(worker.getHashTableStats(qid)).build();
  }

  /**
   * @return an HTTP OK
   */
  @GET
  @Path("/sysgc")
  public Response callSysGC() {
    System.gc();
    return Response.ok().build();
  }

  /**
   * @return the working directory of the Myria worker.
   */
  @GET
  @Path("/working_dir")
  public Response getWorkingDir() {
    return Response.ok(Paths.get(".").toAbsolutePath().normalize().toString()).build();
  }
}
