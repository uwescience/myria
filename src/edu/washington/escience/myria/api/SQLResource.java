/**
 *
 */
package edu.washington.escience.myria.api;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import org.slf4j.LoggerFactory;

import com.wordnik.swagger.annotations.Api;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.parallel.Server;

/**
 *
 */
/**
 * This is the class that handles API calls to create or fetch functions.
 */
@Consumes(MediaType.TEXT_PLAIN)
@Produces(MyriaApiConstants.JSON_UTF_8)
@Path("/sql")
@Api(value = "/sql", description = "Executes SQL in postgres")
public class SQLResource {
  /** The Myria server running on the master. */
  @Context private Server server;
  /** Information about the URL of the request. */

  /** Logger. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FunctionResource.class);

  @POST
  public Response createSQL(final String SQLQuery) throws DbException {
    try {
      server.executeSQLStatement(SQLQuery);
    } catch (Exception e) {
      throw new DbException(e);
    }
    /* Build the response to return the queryId */
    ResponseBuilder response = Response.ok();
    return response.build();
    //return response.entity(functionCreationResponse).build();
  }
}
