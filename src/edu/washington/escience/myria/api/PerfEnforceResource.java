package edu.washington.escience.myria.api;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
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
import edu.washington.escience.myria.perfenforce.encoding.InitializeScalingEncoding;
import edu.washington.escience.myria.perfenforce.encoding.ScalingAlgorithmEncoding;

/**
 * This is the class that handles API calls for PerfEnforce
 */

@Path("/perfenforce")
public class PerfEnforceResource {

  /** The Myria server running on the master. */
  @Context
  private Server server;
  /** Information about the URL of the request. */
  @Context
  private UriInfo uriInfo;

  /** Logger. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DatasetResource.class);

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
   * makes a step, depending on the algorithm chosen
   */
  @POST
  @Path("/step-fake")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response scalingStepFake(@FormDataParam("pathName") final String path, @FormDataParam("seq") final String seq,
      @FormDataParam("scaling") final ScalingAlgorithmEncoding scalingAlgorithmEncoding) {
    LOGGER.warn("Run Fake Query....");
    server.postFakeQuery(path, seq, scalingAlgorithmEncoding);

    /* response */
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
  public Response isEnabled() {
    return Response.ok("Testing").build();
  }

}
