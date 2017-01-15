package edu.washington.escience.myria.api;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.api.encoding.PerfEnforceQueryMetadataEncoding;
import edu.washington.escience.myria.api.encoding.PerfEnforceTableEncoding;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.perfenforce.PerfEnforceException;

/**
 * This is the class that handles API calls for PerfEnforce
 */
@Path("/perfenforce")
public final class PerfEnforceResource {

  @Context private Server server;

  /** Logger. */
  protected static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(PerfEnforceResource.class);

  /**
   * Begins generating a PSLA based on the input schema
   *
   * @param tableList describes all the tables and schema of the dataset
   */
  @POST
  @Path("/preparePSLA")
  public Response prepareData(final List<PerfEnforceTableEncoding> tableList)
      throws PerfEnforceException, DbException, Exception {
    server.getPerfEnforceDriver().preparePSLA(tableList);
    return Response.noContent().build();
  }

  /**
   * Returns the status of the PSLA generation. This is primarily used for the web interface.
   */
  @GET
  @Path("/getDataPreparationStatus")
  public Response getDataPreparationStatus() {
    return Response.ok(server.getPerfEnforceDriver().getDataPreparationStatus()).build();
  }

  /**
   * Returns true if the PSLA generation is finished.
   */
  @GET
  @Path("/isDonePSLA")
  public Response isDonePSLA() {
    return Response.ok(server.getPerfEnforceDriver().isDonePSLA()).build();
  }

  /**
   * Returns the PSLA generated.
   */
  @GET
  @Path("/getPSLA")
  public Response getPSLA() throws PerfEnforceException {
    return Response.ok(server.getPerfEnforceDriver().getPSLA()).build();
  }

  /**
   * Sets the tier the user decides to purchase.
   *
   * @param tier the tier selected by the user
   */
  @POST
  @Path("/setTier")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response setTier(@FormDataParam("tier") final int tier) {
    server.getPerfEnforceDriver().setTier(tier);
    return Response.noContent().build();
  }

  /**
   * Determines the SLA for a given query. The SLA returned is based on the tier purchased.
   *
   * @param querySQL the query from the user
   */
  @POST
  @Path("/findSLA")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response findSLA(@FormDataParam("querySQL") final String querySQL) throws Exception {
    server.getPerfEnforceDriver().findSLA(querySQL);
    return Response.noContent().build();
  }

  /**
   * Records the runtime of a query. This helps the learner adapt.
   *
   * @param queryRuntime the runtime of the query (in seconds).
   */
  @POST
  @Path("/recordRealRuntime")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response recordRealRuntime(@FormDataParam("dataPointRuntime") final Double queryRuntime)
      throws PerfEnforceException {
    server.getPerfEnforceDriver().recordRealRuntime(queryRuntime);
    return Response.noContent().build();
  }

  /**
   * Returns the current cluster size.
   */
  @GET
  @Path("/getClusterSize")
  public Response getClusterSize() {
    return Response.ok(server.getPerfEnforceDriver().getClusterSize()).build();
  }

  /**
   * Assuming the SLA for a query has been determined, this method return metadata about the query the user will execute
   */
  @GET
  @Path("/getCurrentQuery")
  public PerfEnforceQueryMetadataEncoding getCurrentQuery() {
    return server.getPerfEnforceDriver().getCurrentQuery();
  }
}
