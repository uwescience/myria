package edu.washington.escience.myria.api;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Date;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.joda.time.DateTime;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.parallel.Server;

/**
 * Class that handles logs.
 */
@Produces(MediaType.TEXT_PLAIN)
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
   * @param request the current request.
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Path("profiling")
  public Response getProfileLogs(@QueryParam("queryId") final Long queryId,
      @QueryParam("fragmentId") final Long fragmentId, @QueryParam("start") final Long start,
      @QueryParam("end") final Long end, @DefaultValue("0") @QueryParam("minLength") final Long minLength,
      @DefaultValue("false") @QueryParam("onlyRootOp") final boolean onlyRootOp, @Context final Request request)
      throws DbException {

    Preconditions.checkArgument(queryId != null, "Missing required field queryId.");
    Preconditions.checkArgument(fragmentId != null, "Missing required field fragmentId.");
    Preconditions.checkArgument(start != null, "Missing required field start.");
    Preconditions.checkArgument(end != null, "Missing required field end.");
    Preconditions.checkArgument(minLength >= 0, "MinLength has to be greater than or equal to 0");

    EntityTag eTag = new EntityTag(Integer.toString(Joiner.on('-').join("profiling", queryId, fragmentId).hashCode()));
    Object obj = checkAndAddCache(request, eTag);
    if (obj instanceof Response) {
      return (Response) obj;
    }
    ResponseBuilder response = (ResponseBuilder) obj;

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

    server.startLogDataStream(queryId, fragmentId, start, end, minLength, onlyRootOp, writer);
    return response.build();
  }

  /**
   * Get contribution of each operator to runtime.
   * 
   * @param queryId query id.
   * @param fragmentId the fragment id, default is all.
   * @param request the current request.
   * @return the contributions across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Path("contribution")
  public Response getContributions(@QueryParam("queryId") final Long queryId,
      @DefaultValue("-1") @QueryParam("fragmentId") final Long fragmentId, @Context final Request request)
      throws DbException {

    Preconditions.checkArgument(queryId != null, "Missing required field queryId.");

    EntityTag eTag =
        new EntityTag(Integer.toString(Joiner.on('-').join("contribution", queryId, fragmentId).hashCode()));
    Object obj = checkAndAddCache(request, eTag);
    if (obj instanceof Response) {
      return (Response) obj;
    }
    ResponseBuilder response = (ResponseBuilder) obj;

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

    server.startContributionsStream(queryId, fragmentId, writer);

    return response.build();
  }

  /**
   * Get information about where tuples were sent.
   * 
   * @param queryId query id.
   * @param fragmentId the fragment id. < 0 means all
   * @param request the current request.
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Path("sent")
  public Response getSentLogs(@QueryParam("queryId") final Long queryId,
      @DefaultValue("-1") @QueryParam("fragmentId") final long fragmentId, @Context final Request request)
      throws DbException {

    Preconditions.checkArgument(queryId != null, "Missing required field queryId.");

    EntityTag eTag = new EntityTag(Integer.toString(Joiner.on('-').join("sent", queryId, fragmentId).hashCode()));
    Object obj = checkAndAddCache(request, eTag);
    if (obj instanceof Response) {
      return (Response) obj;
    }
    ResponseBuilder response = (ResponseBuilder) obj;

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

    server.startSentLogDataStream(queryId, fragmentId, writer);

    return response.build();
  }

  /**
   * @param queryId query id.
   * @param fragmentId the fragment id.
   * @param request the current request.
   * @return the range for which we have profiling info
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Path("range")
  public Response getRange(@QueryParam("queryId") final Long queryId, @QueryParam("fragmentId") final Long fragmentId,
      @Context final Request request) throws DbException {

    Preconditions.checkArgument(queryId != null, "Missing required field queryId.");
    Preconditions.checkArgument(fragmentId != null, "Missing required field fragmentId.");

    EntityTag eTag = new EntityTag(Integer.toString(Joiner.on('-').join("range", queryId, fragmentId).hashCode()));
    Object obj = checkAndAddCache(request, eTag);
    if (obj instanceof Response) {
      return (Response) obj;
    }
    ResponseBuilder response = (ResponseBuilder) obj;

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

    server.startRangeDataStream(queryId, fragmentId, writer);

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
   * @param request the current request.
   * @return the profiling logs of the query across all workers
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Path("histogram")
  public Response getHistogram(@QueryParam("queryId") final Long queryId,
      @QueryParam("fragmentId") final Long fragmentId, @QueryParam("start") final Long start,
      @QueryParam("end") final Long end, @QueryParam("step") final Long step,
      @DefaultValue("true") @QueryParam("onlyRootOp") final boolean onlyRootOp, @Context final Request request)
      throws DbException {

    Preconditions.checkArgument(queryId != null, "Missing required field queryId.");
    Preconditions.checkArgument(fragmentId != null, "Missing required field fragmentId.");
    Preconditions.checkArgument(start != null, "Missing required field start.");
    Preconditions.checkArgument(end != null, "Missing required field end.");
    Preconditions.checkArgument(step != null, "Missing required field step.");

    EntityTag eTag =
        new EntityTag(Integer.toString(Joiner.on('-').join("histogram", queryId, fragmentId, start, end, step,
            onlyRootOp).hashCode()));

    Object obj = checkAndAddCache(request, eTag);
    if (obj instanceof Response) {
      return (Response) obj;
    }
    ResponseBuilder response = (ResponseBuilder) obj;

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

    server.startHistogramDataStream(queryId, fragmentId, start, end, step, onlyRootOp, writer);

    return response.build();
  }

  /**
   * @param queryId the query id.
   * @param request the current request.
   * @return the resource usage of the query.
   * @throws DbException if there is an error in the database.
   */
  @GET
  @Path("/resourceUsage-{queryId:\\d+}")
  public Response getResourceUsage(@PathParam("queryId") final Long queryId, @Context final Request request)
      throws DbException {

    Preconditions.checkArgument(queryId != null, "Missing required field queryId.");

    ResponseBuilder response = Response.ok().cacheControl(MyriaApiUtils.doNotCache());
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

    server.getResourceUsage(queryId, writerOutput);
    return response.build();
  }

  /**
   * Checks whether the response was cached by the client (checking eTag) and whether it is not too old (checking last
   * modified). Returns a {@link Response} if the content is cached and a {@link ResponseBuilder} if not. In the second
   * case, the caller needs to add to the builder and then return it.
   * 
   * @param request the request
   * @param eTag a unique identifier for the version of the resource to be cached
   * @return {@link Response} if the content is cached and a {@link ResponseBuilder} otherwise
   */
  private Object checkAndAddCache(final Request request, final EntityTag eTag) {
    ResponseBuilder response =
        request.evaluatePreconditions(new DateTime().minus(MyriaConstants.PROFILING_CACHE_AGE).toDate(), eTag);
    if (response != null) {
      return response.build();
    }
    return Response.ok().tag(eTag).lastModified(new Date());
  }
}
