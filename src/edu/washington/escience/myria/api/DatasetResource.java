package edu.washington.escience.myria.api;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.util.List;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import com.sun.jersey.core.header.ContentDisposition;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.JsonTupleWriter;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.api.encoding.DatasetEncoding;
import edu.washington.escience.myria.api.encoding.DatasetStatus;
import edu.washington.escience.myria.api.encoding.TipsyDatasetEncoding;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.operator.FileScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.TipsyFileScan;
import edu.washington.escience.myria.parallel.Server;

/**
 * This is the class that handles API calls to create or fetch datasets.
 * 
 * @author dhalperi
 */
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("/dataset")
public final class DatasetResource {
  /** The Myria server running on the master. */
  @Context
  private Server server;
  /** Information about the URL of the request. */
  @Context
  private UriInfo uriInfo;

  /**
   * @param userName the user who owns the target relation.
   * @param programName the program to which the target relation belongs.
   * @param relationName the name of the target relation.
   * @return metadata about the specified relation.
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Path("/user-{user_name}/program-{program_name}/relation-{relation_name}")
  public Response getDataset(@PathParam("user_name") final String userName,
      @PathParam("program_name") final String programName, @PathParam("relation_name") final String relationName)
      throws DbException {
    DatasetStatus status = server.getDatasetStatus(RelationKey.of(userName, programName, relationName));
    if (status == null) {
      /* Not found, throw a 404 (Not Found) */
      throw new MyriaApiException(Status.NOT_FOUND, "That dataset was not found");
    }
    status.setUri(getCanonicalResourcePath(uriInfo, status.getRelationKey()));
    /* Yay, worked! */
    return Response.ok(status).build();
  }

  /**
   * Helper function to parse a format string, with default value "csv".
   * 
   * @param format the format string, with default value "csv".
   * @return the cleaned-up format string.
   */
  private String validateFormat(final String format) {
    String cleanFormat = format;
    if (cleanFormat == null) {
      cleanFormat = "csv";
    }
    cleanFormat = cleanFormat.trim().toLowerCase();
    /* CSV is legal */
    if (cleanFormat.equals("csv")) {
      return cleanFormat;
    }
    /* TSV is legal */
    if (cleanFormat.equals("tsv")) {
      return cleanFormat;
    }
    /* JSON is legal */
    if (cleanFormat.equals("json")) {
      return cleanFormat;
    }
    throw new MyriaApiException(Status.BAD_REQUEST, "format must be 'csv', 'tsv', or 'json'");
  }

  /**
   * @param userName the user who owns the target relation.
   * @param programName the program to which the target relation belongs.
   * @param relationName the name of the target relation.
   * @param format the format of the output data. Valid options are (case-insensitive) "csv", "tsv", and "json".
   * @return metadata about the specified relation.
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Produces({ MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON })
  @Path("/user-{user_name}/program-{program_name}/relation-{relation_name}/data")
  public Response getDatasetData(@PathParam("user_name") final String userName,
      @PathParam("program_name") final String programName, @PathParam("relation_name") final String relationName,
      @QueryParam("format") final String format) throws DbException {

    /* Start building the response. */
    ResponseBuilder response = Response.ok();

    /* Assemble the name of the relation. */
    RelationKey relationKey = RelationKey.of(userName, programName, relationName);

    /* Validate the request format. This will throw a MyriaApiException if format is invalid. */
    String validFormat = validateFormat(format);

    /*
     * Allocate the pipes by which the {@link DataOutput} operator will talk to the {@link StreamingOutput} object that
     * will stream data to the client.
     */
    PipedOutputStream writerOutput = new PipedOutputStream();
    PipedInputStream input;
    try {
      input = new PipedInputStream(writerOutput, MyriaConstants.DEFAULT_PIPED_INPUT_STREAM_SIZE);
    } catch (IOException e) {
      throw new DbException(e);
    }

    /* Create a {@link PipedStreamingOutput} object that will stream the serialized results to the client. */
    PipedStreamingOutput entity = new PipedStreamingOutput(input);
    /* .. and make it the entity of the response. */
    response.entity(entity);

    /* Set up the TupleWriter and the Response MediaType based on the format choices. */
    TupleWriter writer;
    if (validFormat.equals("csv") || validFormat.equals("tsv")) {
      /* CSV or TSV : set application/octet-stream, attachment, and filename. */
      if (validFormat.equals("csv")) {
        writer = new CsvTupleWriter(writerOutput);
      } else {
        writer = new CsvTupleWriter('\t', writerOutput);
      }
      ContentDisposition contentDisposition =
          ContentDisposition.type("attachment").fileName(
              relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + '.' + validFormat).build();

      response.header("Content-Disposition", contentDisposition);
      response.type(MediaType.APPLICATION_OCTET_STREAM);
    } else if (validFormat.equals("json")) {
      /* JSON: set application/json. */
      response.type(MediaType.APPLICATION_JSON);
      writer = new JsonTupleWriter(writerOutput);
    } else {
      /* Should not be possible to get here. */
      throw new IllegalStateException("format should have been validated by now, and yet we got here");
    }

    /* Start streaming tuples into the TupleWriter, and through the pipes to the PipedStreamingOutput. */
    try {
      server.startDataStream(relationKey, writer);
    } catch (IllegalArgumentException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }

    /* Yay, worked! Ensure the file has the correct filename. */
    return response.build();
  }

  /**
   * @param dataset the dataset to be ingested.
   * @return the created dataset resource.
   * @throws DbException if there is an error in the database.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response newDataset(final DatasetEncoding dataset) throws DbException {
    dataset.validate();

    /* If we already have a dataset by this name, tell the user there's a conflict. */
    try {
      if (server.getSchema(dataset.relationKey) != null) {
        /* Found, throw a 409 (Conflict) */
        throw new MyriaApiException(Status.CONFLICT, "That dataset already exists.");
      }
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    /* If we don't have any workers alive right now, tell the user we're busy. */
    Set<Integer> workers = server.getAliveWorkers();
    if (workers.size() == 0) {
      /* Throw a 503 (Service Unavailable) */
      throw new MyriaApiException(Status.SERVICE_UNAVAILABLE, "There are no alive workers to receive this dataset.");
    }

    /* Moreover, check whether all requested workers are alive. */
    if (dataset.workers != null && !server.getAliveWorkers().containsAll(dataset.workers)) {
      /* Throw a 503 (Service Unavailable) */
      throw new MyriaApiException(Status.SERVICE_UNAVAILABLE, "Not all requested workers are alive");
    }

    /* Do the work. */
    Operator source = new FileScan(dataset.source, dataset.schema);
    DatasetStatus status = null;
    try {
      status = server.ingestDataset(dataset.relationKey, dataset.workers, source);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    /* In the response, tell the client the path to the relation. */
    URI datasetUri = getCanonicalResourcePath(uriInfo, dataset.relationKey);
    status.setUri(datasetUri);
    return Response.created(datasetUri).entity(status).build();
  }

  /**
   * @param dataset the dataset to be imported.
   * @param uriInfo information about the current URL.
   * @return created dataset resource.
   * @throws DbException if there is an error in the database.
   */
  @POST
  @Path("/importDataset")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response importDataset(final DatasetEncoding dataset, @Context final UriInfo uriInfo) throws DbException {

    /* If we already have a dataset by this name, tell the user there's a conflict. */
    try {
      if (server.getSchema(dataset.relationKey) != null) {
        /* Found, throw a 409 (Conflict) */
        throw new MyriaApiException(Status.CONFLICT, "That dataset already exists.");
      }
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    /* Moreover, check whether all requested workers are valid. */
    if (dataset.workers != null && !server.getWorkers().keySet().containsAll(dataset.workers)) {
      /* Throw a 503 (Service Unavailable) */
      throw new MyriaApiException(Status.SERVICE_UNAVAILABLE, "Do not specify the workers of the dataset correctly");
    }

    try {
      server.importDataset(dataset.relationKey, dataset.schema, dataset.workers);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    /* In the response, tell the client the path to the relation. */
    return Response.created(getCanonicalResourcePath(uriInfo, dataset.relationKey)).build();

  }

  /**
   * @param dataset the dataset to be ingested.
   * @param uriInfo information about the current URL.
   * @return the created dataset resource.
   * @throws DbException if there is an error in the database.
   */
  @POST
  @Path("/tipsy")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response newTipsyDataset(final TipsyDatasetEncoding dataset, @Context final UriInfo uriInfo)
      throws DbException {
    dataset.validate();

    /* If we already have a dataset by this name, tell the user there's a conflict. */
    try {
      if (server.getSchema(dataset.relationKey) != null) {
        /* Found, throw a 409 (Conflict) */
        throw new MyriaApiException(Status.CONFLICT, "That dataset already exists.");
      }
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    /* If we don't have any workers alive right now, tell the user we're busy. */
    Set<Integer> workers = server.getAliveWorkers();
    if (workers.size() == 0) {
      /* Throw a 503 (Service Unavailable) */
      throw new MyriaApiException(Status.SERVICE_UNAVAILABLE, "There are no alive workers to receive this dataset.");
    }

    /* Moreover, check whether all requested workers are alive. */
    if (dataset.workers != null && !server.getAliveWorkers().containsAll(dataset.workers)) {
      /* Throw a 503 (Service Unavailable) */
      throw new MyriaApiException(Status.SERVICE_UNAVAILABLE, "Not all requested workers are alive");
    }

    /* Do the work. */
    try {
      server.ingestDataset(dataset.relationKey, dataset.workers, new TipsyFileScan(dataset.tipsyFilename,
          dataset.iorderFilename, dataset.grpFilename));
    } catch (InterruptedException ee) {
      Thread.currentThread().interrupt();
      ee.printStackTrace();
    }

    /* In the response, tell the client the path to the relation. */
    return Response.created(getCanonicalResourcePath(uriInfo, dataset.relationKey)).build();
  }

  /**
   * @return a list of datasets in the system.
   * @throws DbException if there is an error accessing the Catalog.
   */
  @GET
  public Response getDatasets() throws DbException {
    List<DatasetStatus> datasets = server.getDatasets();
    for (DatasetStatus status : datasets) {
      status.setUri(getCanonicalResourcePath(uriInfo, status.getRelationKey()));
    }
    return Response.ok().cacheControl(MyriaApiUtils.doNotCache()).entity(datasets).build();
  }

  /**
   * @param userName the user whose datasets we want to access.
   * @return a list of datasets belonging to the specified user.
   * @throws DbException if there is an error accessing the Catalog.
   */
  @GET
  @Path("/user-{user_name}")
  public List<DatasetStatus> getDatasetsForUser(@PathParam("user_name") final String userName) throws DbException {
    List<DatasetStatus> datasets = server.getDatasetsForUser(userName);
    for (DatasetStatus status : datasets) {
      status.setUri(getCanonicalResourcePath(uriInfo, status.getRelationKey()));
    }
    return datasets;
  }

  /**
   * @param userName the user whose datasets we want to access.
   * @param programName the program owned by that user whose datasets we want to access.
   * @return a list of datasets in the program.
   * @throws DbException if there is an error accessing the Catalog.
   */
  @GET
  @Path("/user-{user_name}/program-{program_name}")
  public List<DatasetStatus> getDatasetsForUser(@PathParam("user_name") final String userName,
      @PathParam("program_name") final String programName) throws DbException {
    List<DatasetStatus> datasets = server.getDatasetsForProgram(userName, programName);
    for (DatasetStatus status : datasets) {
      status.setUri(getCanonicalResourcePath(uriInfo, status.getRelationKey()));
    }
    return datasets;
  }

  /**
   * @param uriInfo information about the URL of the request.
   * @return the canonical URL for this API.
   */
  public static URI getCanonicalResourcePath(final UriInfo uriInfo) {
    return getCanonicalResourcePathBuilder(uriInfo).build();
  }

  /**
   * @param uriInfo information about the URL of the request.
   * @return a builder for the canonical URL for this API.
   */
  public static UriBuilder getCanonicalResourcePathBuilder(final UriInfo uriInfo) {
    return uriInfo.getBaseUriBuilder().path("/dataset");
  }

  /**
   * @param uriInfo information about the URL of the request.
   * @param relationKey the key of the relation.
   * @return the canonical URL for this API.
   */
  public static URI getCanonicalResourcePath(final UriInfo uriInfo, final RelationKey relationKey) {
    return getCanonicalResourcePathBuilder(uriInfo, relationKey).build();
  }

  /**
   * @param uriInfo information about the URL of the request.
   * @param relationKey the key of the relation.
   * @return a builder for the canonical URL for this API.
   */
  public static UriBuilder getCanonicalResourcePathBuilder(final UriInfo uriInfo, final RelationKey relationKey) {
    return getCanonicalResourcePathBuilder(uriInfo).path("user-" + relationKey.getUserName()).path(
        "program-" + relationKey.getProgramName()).path("relation-" + relationKey.getRelationName());
  }
}
