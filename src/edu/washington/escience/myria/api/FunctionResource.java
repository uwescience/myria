/**
 *
 */
package edu.washington.escience.myria.api;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.LoggerFactory;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.api.encoding.FunctionEncoding;
import edu.washington.escience.myria.api.encoding.FunctionStatus;
import edu.washington.escience.myria.parallel.Server;

/**
 * 
 */
/**
 * This is the class that handles API calls to create or fetch functions.
 *
 */
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MyriaApiConstants.JSON_UTF_8)
@Path("/function")
@Api(value = "/function", description = "Operations on functions")
public class FunctionResource {
  /** The Myria server running on the master. */
  @Context
  private Server server;
  /** Information about the URL of the request. */

  /** Logger. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FunctionResource.class);

  /**
   * @param queryId an optional query ID specifying which datasets to get.
   * @return a list of datasets.
   * @throws DbException if there is an error accessing the Catalog.
   */
  @GET
  public List<String> getFunctions() throws DbException {
    LOGGER.info("get functions!");
    return server.getFunctions();
  }

  /**
   * Creates an Function based on DbFunctionEncoding
   * 
   * @POST
   */
  @POST
  @Path("/register")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createFunction(final FunctionEncoding encoding) throws DbException {
    LOGGER.info("register function!");

    long queryId;
    try {
      queryId =
          server.createFunction(encoding.name, encoding.text, encoding.lang, encoding.workers, encoding.binary,
              encoding.inputSchema, encoding.outputSchema);

    } catch (Exception e) {
      throw new DbException();
    }
    /* Build the response to return the queryId */
    ResponseBuilder response = Response.ok();
    return response.entity(queryId).build();
  }

  @GET
  @ApiOperation(value = "get information about a function", response = FunctionStatus.class)
  @ApiResponses(value = { @ApiResponse(code = HttpStatus.SC_NOT_FOUND, message = "Function not found", response = String.class) })
  @Produces({ MediaType.APPLICATION_OCTET_STREAM, MyriaApiConstants.JSON_UTF_8 })
  @Path("/{name}")
  public Response getFunction(@PathParam("name") final String name) throws DbException, IOException {

    /* Assemble the name of the relation. */
    String functionName = name;
    LOGGER.info("get function!");

    LOGGER.info("looking for  function with name: " + functionName);
    FunctionStatus status = server.getFunctionDetails(functionName);
    if (status == null) {
      /* Not found, throw a 404 (Not Found) */
      throw new MyriaApiException(Status.NOT_FOUND, "That function was not found");
    }

    return Response.ok(status).build();
  }

}
