package edu.washington.escience.myria.api;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import javax.ws.rs.Consumes;
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

import org.slf4j.LoggerFactory;

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
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LogResource.class);
  /** The Myria server running on the master. */
  @Context
  private Server server;

  /**
   * Get profiling logs of a query.
   * 
   * @param queryId query id.
   * @param uriInfo the URL of the current request.
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Produces({ MediaType.TEXT_PLAIN })
  @Path("profiling")
  public Response getProfileLogs(@QueryParam("queryId") final long queryId, @Context final UriInfo uriInfo)
      throws DbException {
    return getLogs(queryId, MyriaConstants.PROFILING_RELATION, MyriaConstants.PROFILING_SCHEMA);
  }

  /**
   * Get sent logs of a query.
   * 
   * @param queryId query id.
   * @param uriInfo the URL of the current request.
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Produces({ MediaType.TEXT_PLAIN })
  @Path("sent")
  public Response getSentLogs(@QueryParam("queryId") final long queryId, @Context final UriInfo uriInfo)
      throws DbException {
    return getLogs(queryId, MyriaConstants.LOG_SENT_RELATION, MyriaConstants.LOG_SENT_SCHEMA);
  }

  /**
   * @param queryId query id
   * @param relationKey the relation to stream from
   * @param schema the schema of the relation to stream from
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  private Response getLogs(final long queryId, final RelationKey relationKey, final Schema schema) throws DbException {
    /* Start building the response. */
    ResponseBuilder response = Response.ok();
    response.type(MediaType.TEXT_PLAIN);
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
    TupleWriter writer = new CsvTupleWriter(writerOutput);

    /* Start streaming tuples into the TupleWriter, and through the pipes to the PipedStreamingOutput. */
    try {
      server.startLogDataStream(queryId, writer, relationKey, schema);
    } catch (IllegalArgumentException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }

    /* Yay, worked! Ensure the file has the correct filename. */
    return response.build();
  }
}
