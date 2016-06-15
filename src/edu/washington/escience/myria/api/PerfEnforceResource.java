package edu.washington.escience.myria.api;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.json.JSONException;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.perfenforce.QueryMetaData;
import edu.washington.escience.myria.perfenforce.encoding.InitializeScalingEncoding;
import edu.washington.escience.myria.perfenforce.encoding.ScalingAlgorithmEncoding;
import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * This is the class that handles API calls for PerfEnforce
 */

@Path("/perfenforce")
public final class PerfEnforceResource {

  /** The Myria server running on the master. */
  @Context
  private Server server;
  /** Information about the URL of the request. */
  @Context
  private UriInfo uriInfo;

  /** Logger. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PerfEnforceResource.class);

  @POST
  @Path("/data-preparation")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response prepareData(@FormDataParam("pathName") final String pathName) throws DbException, JSONException,
      IOException, InterruptedException, ExecutionException, CatalogException {

    LOGGER.warn("PATH PASSED IN: " + pathName);
    server.preparePerfEnforce(pathName);
    /* response */
    return Response.noContent().build();
  }

  /*
   * Must post the tier selected and scaling algorithm, also makes sure to
   */
  @POST
  @Path("/initializeScaling")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response initializeScalingAlgorithm(final InitializeScalingEncoding scalingEncoding) {

    LOGGER.warn("Begin Monitoring....");
    server.beginMonitoring(scalingEncoding);

    /* response */
    return Response.noContent().build();
  }

  /*
   * prepares the next query
   */
  @POST
  @Path("/setup-fake")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response scalingStepFake(final ScalingAlgorithmEncoding scalingAlgorithmEncoding) {
    server.postSetupNextFakeQuery(scalingAlgorithmEncoding);

    /* response */
    return Response.noContent().build();
  }

  /*
   * runs an iteration, depending on the algorithm initialized
   */
  @POST
  @Path("/step-iteration")
  public Response postRunIterationQuery() {
    server.postRunIterationQuery();
    return Response.noContent().build();
  }

  @GET
  @Path("/cluster-size")
  public Response getClusterSize() throws DbException {
    return Response.ok(server.getClusterSize()).build();
  }

  @GET
  @Path("/current-query-ideal")
  public Response getCurrentQueryIdealSize() throws DbException {
    return Response.ok(server.getCurrentQueryIdealSize()).build();
  }

  @GET
  @Path("/scaling-algorithm-state")
  @Produces(MediaType.APPLICATION_JSON)
  public ScalingStatusEncoding getScalingStatus() throws DbException {
    return server.getScalingStatus();
  }

  @GET
  @Path("/get-previous-query")
  public QueryMetaData getPreviousQuery() throws DbException {
    return server.getPreviousQuery();
  }

  @GET
  @Path("/get-current-query")
  public QueryMetaData getCurrentQuery() throws DbException {
    return server.getCurrentQuery();
  }

}
