package edu.washington.escience.myriad.api;

import java.io.IOException;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;

/**
 * This is the class that handles API calls that return relation schemas.
 * 
 * @author dhalperi
 */
@Produces(MediaType.APPLICATION_JSON)
@Path("/dataset")
public final class DatasetResource {
  /**
   * @param userName the user who owns the target relation.
   * @param programName the program to which the target relation belongs.
   * @param relationName the name of the target relation.
   * @return the schema of the specified relation.
   */
  @GET
  @Path("/user-{userName}/program-{programName}/relation-{relationName}")
  public Schema getDataset(@PathParam("userName") final String userName,
      @PathParam("programName") final String programName, @PathParam("relationName") final String relationName) {
    try {
      Schema schema = MyriaApiUtils.getServer().getSchema(RelationKey.of(userName, programName, relationName));
      if (schema == null) {
        /* Not found, throw a 404 (Not Found) */
        throw new WebApplicationException(Response.status(Status.NOT_FOUND).build());
      }
      /* Yay, worked! */
      return schema;
    } catch (final CatalogException e) {
      /* Throw a 500 (Internal Server Error) */
      throw new WebApplicationException(Response.status(Status.INTERNAL_SERVER_ERROR).build());
    }
  }

  /**
   * @param uriInfo information about the current URL.
   * @param payload the request payload.
   * @return the created dataset resource.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response newDataset(final byte[] payload, @Context final UriInfo uriInfo) {
    if (payload == null || payload.length == 0) {
      throw new WebApplicationException(Response.status(Status.BAD_REQUEST).entity("Payload cannot be empty.\n")
          .build());
    }

    /* Attempt to deserialize the object. */
    DatasetEncoding dataset;
    try {
      final ObjectMapper mapper = new ObjectMapper();
      dataset = mapper.readValue(payload, DatasetEncoding.class);
    } catch (IOException e) {
      throw new WebApplicationException(Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build());
    }

    /* If we already have a dataset by this name, tell the user there's a conflict. */
    try {
      if (MyriaApiUtils.getServer().getSchema(dataset.relationKey) != null) {
        /* Found, throw a 409 (Conflict) */
        throw new WebApplicationException(Response.status(Status.CONFLICT).build());
      }
    } catch (final CatalogException e) {
      /* Throw a 500 (Internal Server Error) */
      throw new WebApplicationException(Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
    }

    /* If we don't have any workers alive right now, tell the user we're busy. */
    Set<Integer> workers = MyriaApiUtils.getServer().getAliveWorkers();
    if (workers.size() == 0) {
      /* Throw a 503 (Service Unavailable) */
      throw new WebApplicationException(Response.status(Status.SERVICE_UNAVAILABLE).build());
    }

    /* Do the work. */
    try {
      MyriaApiUtils.getServer().ingestDataset(dataset.relationKey, dataset.schema, dataset.data);
    } catch (CatalogException e) {
      throw new WebApplicationException(Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
    } catch (InterruptedException ee) {
      Thread.currentThread().interrupt();
      ee.printStackTrace();
    }

    /* In the response, tell the client the path to the relation. */
    UriBuilder queryUri = uriInfo.getBaseUriBuilder();
    return Response.created(
        queryUri.path("dataset").path("user-" + dataset.relationKey.getUserName()).path(
            "program-" + dataset.relationKey.getProgramName())
            .path("relation-" + dataset.relationKey.getRelationName()).build()).build();
  }

  /**
   * A JSON-able wrapper for the expected wire message for a new dataset.
   * 
   */
  class DatasetEncoding {
    /** The name of the dataset. */
    @JsonProperty("relation_key")
    public RelationKey relationKey;
    /** The Schema of its tuples. */
    @JsonProperty("schema")
    public Schema schema;
    /** The data it contains. */
    @JsonProperty("data")
    public byte[] data;
  }
}
