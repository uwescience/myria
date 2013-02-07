package edu.washington.escience.myriad.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;

/**
 * This is the class that handles API calls that return relation schemas.
 * 
 * @author dhalperi
 */
@Path("/schema")
public final class SchemaResource {
  /**
   * @param userName the user who owns the target relation.
   * @param programName the program to which the target relation belongs.
   * @param relationName the name of the target relation.
   * @return the schema of the specified relation.
   */
  @GET
  @Path("/user-{userName}/program-{programName}/relation-{relationName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Schema getWorker(@PathParam("userName") final String userName,
      @PathParam("programName") final String programName, @PathParam("relationName") final String relationName) {
    Schema schema;
    try {
      schema = MasterApiServer.getMyriaServer().getSchema(RelationKey.of(userName, programName, relationName));
    } catch (final CatalogException e) {
      /* Throw a 500 (Internal Server Error) */
      throw new WebApplicationException(Response.status(Status.INTERNAL_SERVER_ERROR).build());
    }
    if (schema == null) {
      /* Not found, throw a 404 (Not Found) */
      throw new WebApplicationException(Response.status(Status.NOT_FOUND).build());
    }
    /* Yay, worked! */
    return schema;
  }
}
