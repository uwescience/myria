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

  @POST
  @Path("/preparePSLA")
  public Response prepareData(final List<PerfEnforceTableEncoding> tableList)
      throws PerfEnforceException, DbException, Exception {
    server.getPerfEnforceDriver().preparePSLA(tableList);
    return Response.noContent().build();
  }

  @GET
  @Path("/isDonePSLA")
  public Response isDonePSLA() {
    return Response.ok(server.getPerfEnforceDriver().isDonePSLA()).build();
  }

  @POST
  @Path("/setTier")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response setTier(@FormDataParam("tier") final int queryRuntime) {
    server.getPerfEnforceDriver().setTier(queryRuntime);
    return Response.noContent().build();
  }

  @POST
  @Path("/findSLA")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response findSLA(@FormDataParam("querySQL") final String querySQL)
      throws PerfEnforceException {
    server.getPerfEnforceDriver().findSLA(querySQL);
    return Response.noContent().build();
  }

  @POST
  @Path("/recordRealRuntime")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response recordRealRuntime(@FormDataParam("dataPointRuntime") final Double queryRuntime)
      throws PerfEnforceException {
    server.getPerfEnforceDriver().recordRealRuntime(queryRuntime);
    return Response.noContent().build();
  }

  @GET
  @Path("/getClusterSize")
  public Response getClusterSize() {
    return Response.ok(server.getPerfEnforceDriver().getClusterSize()).build();
  }

  @GET
  @Path("/getPreviousQuery")
  public PerfEnforceQueryMetadataEncoding getPreviousQuery() {
    return server.getPerfEnforceDriver().getPreviousQuery();
  }

  @GET
  @Path("/getCurrentQuery")
  public PerfEnforceQueryMetadataEncoding getCurrentQuery() {
    return server.getPerfEnforceDriver().getCurrentQuery();
  }
}
