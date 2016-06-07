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
import com.wordnik.swagger.annotations.Api;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.perfenforce.encoding.InitializeScalingEncoding;

/**
 * This is the class that handles API calls for PerfEnforce
 */

@Path("/perfenforce")
@Api(value = "/perfenforce", description = "Perfenforce Operations")
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
  public Response prepareData(@FormDataParam("filename") final String filename) throws DbException, JSONException,
      IOException, InterruptedException, ExecutionException, CatalogException {

    server.preparePerfEnforce(filename);
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

    server.beginMonitoring(scalingEncoding);

    /* response */
    return Response.noContent().build();
  }

  /*
   * makes a step, depending on the algorithm chosen
   */
  @POST
  @Path("/step")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response scalingStep() {

    // query calls are sent here
    // Post the next query fake or (get the next query)

    // then step() on the perfEnforce Scaling Algorithm

    /* response */
    return Response.noContent().build();
  }

  @GET
  @Path("/cluster-size")
  public Response getClusterSize() throws DbException {
    return Response.ok(server.getClusterSize()).build();
  }
}
