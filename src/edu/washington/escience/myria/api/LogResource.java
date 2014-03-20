package edu.washington.escience.myria.api;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.parallel.Server;

/**
 * Class that handles logs.
 */
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("/logs")
public final class LogResource {
  /** The Myria server running on the master. */
  @Context
  private Server server;

  /**
   * Get profiling logs of a query.
   * 
   * @param queryId query id.
   * @param fragmentId the fragment id. < 0 means all
   * @param uriInfo the URL of the current request.
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("profiling")
  public Response getProfileLogs(@QueryParam("queryId") final Long queryId,
      @DefaultValue("-1") @QueryParam("fragmentId") final long fragmentId, @Context final UriInfo uriInfo)
      throws DbException {
    if (queryId == null) {
      throw new MyriaApiException(Status.BAD_REQUEST, "Query ID missing.");
    }
    return getLogs(queryId, fragmentId, MyriaConstants.PROFILING_RELATION, MyriaConstants.PROFILING_SCHEMA);
  }

  /**
   * Get sent logs of a query.
   * 
   * @param queryId query id.
   * @param fragmentId the fragment id. < 0 means all
   * @param uriInfo the URL of the current request.
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("sent")
  public Response getSentLogs(@QueryParam("queryId") final Long queryId,
      @DefaultValue("-1") @QueryParam("fragmentId") final long fragmentId, @Context final UriInfo uriInfo)
      throws DbException {
    if (queryId == null) {
      throw new MyriaApiException(Status.BAD_REQUEST, "Query ID missing.");
    }
    return getLogs(queryId, fragmentId, MyriaConstants.LOG_SENT_RELATION, MyriaConstants.LOG_SENT_SCHEMA);
  }

  /**
   * @param queryId query id
   * @param fragmentId the fragment id to return data for. All fragments, if < 0.
   * @param relationKey the relation to stream from
   * @param schema the schema of the relation to stream from
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  private Response getLogs(final long queryId, final long fragmentId, final RelationKey relationKey, final Schema schema)
      throws DbException {
    ResponseBuilder response = Response.ok();
    response.type(MediaType.TEXT_PLAIN);

    PipedOutputStream writerOutput = new PipedOutputStream();
    PipedInputStream input;
    try {
      input = new PipedInputStream(writerOutput, MyriaConstants.DEFAULT_PIPED_INPUT_STREAM_SIZE);
    } catch (IOException e) {
      throw new DbException(e);
    }

    PipedStreamingOutput entity = new PipedStreamingOutput(input);
    response.entity(entity);

    TupleWriter writer = new CsvTupleWriter(writerOutput);

    try {
      server.startLogDataStream(queryId, fragmentId, writer, relationKey, schema);
    } catch (IllegalArgumentException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }

    return response.build();
  }

  /**
   * Get the number of workers working on a fragment based on profiling logs of a query for the root operators.
   * 
   * @param queryId query id.
   * @param fragmentId the fragment id.
   * @param uriInfo the URL of the current request.
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("histogram")
  public Response getHistogram(@QueryParam("queryId") final Long queryId,
      @QueryParam("fragmentId") final Long fragmentId, @Context final UriInfo uriInfo) throws DbException {
    if (queryId == null) {
      throw new MyriaApiException(Status.BAD_REQUEST, "Query ID missing.");
    }
    if (fragmentId == null) {
      throw new MyriaApiException(Status.BAD_REQUEST, "Fragment ID missing.");
    }

    ResponseBuilder response = Response.ok();
    response.type(MediaType.TEXT_PLAIN);

    PipedOutputStream writerOutput = new PipedOutputStream();
    PipedInputStream input;
    try {
      input = new PipedInputStream(writerOutput, MyriaConstants.DEFAULT_PIPED_INPUT_STREAM_SIZE);
    } catch (IOException e) {
      throw new DbException(e);
    }

    PipedStreamingOutput entity = new PipedStreamingOutput(input);
    response.entity(entity);

    TupleWriter writer = new CsvTupleWriter(writerOutput);

    try {
      server.startHistogramDataStream(queryId, fragmentId, writer);
    } catch (IllegalArgumentException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }

    return response.build();
  }
}
