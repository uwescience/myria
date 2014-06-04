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

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
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
   * @param fragmentId the fragment id.
   * @param start the earliest time where we need data
   * @param end the latest time
   * @param minLength the minimum length of a span to return, default is 0
   * @param onlyRootOp the operator to return data for, default is all
   * @param uriInfo the URL of the current request
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("profiling")
  public Response getProfileLogs(@QueryParam("queryId") final Long queryId,
      @QueryParam("fragmentId") final Long fragmentId, @QueryParam("start") final Long start,
      @QueryParam("end") final Long end, @DefaultValue("0") @QueryParam("minLength") final Long minLength,
      @DefaultValue("false") @QueryParam("onlyRootOp") final boolean onlyRootOp, @Context final UriInfo uriInfo)
      throws DbException {

    Preconditions.checkArgument(queryId != null, "Missing required field queryId.");
    Preconditions.checkArgument(fragmentId != null, "Missing required field fragmentId.");
    Preconditions.checkArgument(start != null, "Missing required field start.");
    Preconditions.checkArgument(end != null, "Missing required field end.");
    Preconditions.checkArgument(minLength >= 0, "MinLength has to be greater than or equal to 0");

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
      server.startLogDataStream(queryId, fragmentId, start, end, minLength, onlyRootOp, writer);
    } catch (IllegalArgumentException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }

    return response.build();
  }

  /**
   * Get contribution of each operator to runtime.
   * 
   * @param queryId query id.
   * @param fragmentId the fragment id, default is all.
   * @param uriInfo the URL of the current request
   * @return the contributions across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("contribution")
  public Response getContributions(@QueryParam("queryId") final Long queryId,
      @DefaultValue("-1") @QueryParam("fragmentId") final Long fragmentId, @Context final UriInfo uriInfo)
      throws DbException {

    Preconditions.checkArgument(queryId != null, "Missing required field queryId.");

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
      server.startContributionsStream(queryId, fragmentId, writer);
    } catch (IllegalArgumentException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }

    return response.build();
  }

  /**
   * Get information about where tuples were sent.
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

    Preconditions.checkArgument(queryId != null, "Missing required field queryId.");

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
      server.startSentLogDataStream(queryId, fragmentId, writer);
    } catch (IllegalArgumentException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }

    return response.build();
  }

  /**
   * @param queryId query id.
   * @param fragmentId the fragment id.
   * @param uriInfo the URL of the current request.
   * @return the range for which we have profiling info
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("range")
  public Response getRange(@QueryParam("queryId") final Long queryId, @QueryParam("fragmentId") final Long fragmentId,
      @Context final UriInfo uriInfo) throws DbException {

    Preconditions.checkArgument(queryId != null, "Missing required field queryId.");
    Preconditions.checkArgument(fragmentId != null, "Missing required field fragmentId.");

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
      server.startRangeDataStream(queryId, fragmentId, writer);
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
   * @param start the start of the range
   * @param end the end of the range
   * @param step the length of a step
   * @param onlyRootOp return histogram for root operator, default is all
   * @param uriInfo the URL of the current request.
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("histogram")
  public Response getHistogram(@QueryParam("queryId") final Long queryId,
      @QueryParam("fragmentId") final Long fragmentId, @QueryParam("start") final Long start,
      @QueryParam("end") final Long end, @QueryParam("step") final Long step,
      @DefaultValue("true") @QueryParam("onlyRootOp") final boolean onlyRootOp, @Context final UriInfo uriInfo)
      throws DbException {

    Preconditions.checkArgument(queryId != null, "Missing required field queryId.");
    Preconditions.checkArgument(fragmentId != null, "Missing required field fragmentId.");
    Preconditions.checkArgument(start != null, "Missing required field start.");
    Preconditions.checkArgument(end != null, "Missing required field end.");
    Preconditions.checkArgument(step != null, "Missing required field step.");

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
      server.startHistogramDataStream(queryId, fragmentId, start, end, step, onlyRootOp, writer);
    } catch (IllegalArgumentException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }

    return response.build();
  }
}
