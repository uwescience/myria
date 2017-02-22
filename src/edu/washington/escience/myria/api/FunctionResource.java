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
import edu.washington.escience.myria.api.encoding.CreateFunctionEncoding;
import edu.washington.escience.myria.api.encoding.FunctionStatus;
import edu.washington.escience.myria.parallel.Server;

/**
 *
 */
/**
 * This is the class that handles API calls to create or fetch functions.
 */
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MyriaApiConstants.JSON_UTF_8)
@Path("/function")
@Api(value = "/function", description = "Operations on functions")
public class FunctionResource {
  /** The Myria server running on the master. */
  @Context private Server server;
  /** Information about the URL of the request. */

  /** Logger. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FunctionResource.class);

  /**
   * @return a list of function, names only.
   * @throws DbException if there is an error accessing the Catalog.
   */
  @GET
  public List<String> getFunctions() throws DbException {
    return server.getFunctions();
  }

  @POST
  public Response createFunction(final CreateFunctionEncoding encoding) throws DbException {
    long functionCreationResponse;
    try {
      functionCreationResponse =
          server.createFunction(
              encoding.name,
              encoding.description,
              encoding.outputType,
              encoding.isMultiValued,
              encoding.lang,
              encoding.binary,
              encoding.workers);
    } catch (Exception e) {
      throw new DbException(e);
    }
    /* Build the response to return the queryId */
    ResponseBuilder response = Response.ok();
    return response.entity(functionCreationResponse).build();
  }

  /**
   * @param name function name
   * @return details of a registered function.
   * @throws DbException if there is an error accessing the Catalog.
   */
  @GET
  @ApiOperation(value = "Get information about a function.", response = FunctionStatus.class)
  @ApiResponses(
    value = {
      @ApiResponse(
        code = HttpStatus.SC_NOT_FOUND,
        message = "Function not found",
        response = String.class
      )
    }
  )
  @Path("/{name}")
  public Response getFunction(@PathParam("name") final String name)
      throws DbException, IOException {

    String functionName = name;
    FunctionStatus status = server.getFunctionDetails(functionName);
    if (status == null) {
      /* Not found, throw a 404 (Not Found) */
      throw new MyriaApiException(Status.NOT_FOUND, "That function was not found");
    }

    return Response.ok(status).build();
  }
}
