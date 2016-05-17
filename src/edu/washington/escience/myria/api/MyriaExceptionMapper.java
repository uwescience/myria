package edu.washington.escience.myria.api;

import java.io.EOFException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonMappingException;

import edu.washington.escience.myria.util.ErrorUtils;

/**
 * This class determines what HTTP response will be sent when a web request causes a certain exception type. The
 * standard ones are 500 (INTERNAL SERVER ERROR) and 400 (BAD REQUEST).
 *
 * Note that any resource wishing to control its response type directly can simply throw a {@link MyriaApiException},
 * which is a subclass of {@link WebApplicationException} and thus already contains an HTTP Response that will be passed
 * through directly.
 *
 *
 */
@Provider
public final class MyriaExceptionMapper implements ExceptionMapper<Throwable> {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(MyriaExceptionMapper.class);

  @Override
  public Response toResponse(final Throwable ex) {
    /** Any WebApplicationException should be unmodified. **/
    if (ex instanceof WebApplicationException) {
      return ((WebApplicationException) ex).getResponse();
    }
    /** Wrong arguments are probably caused by a bad request. */
    if (ex instanceof IllegalArgumentException) {
      return MyriaExceptionMapper.getResponse(Status.BAD_REQUEST, ex);
    }
    /** Problems during JsonMapping are client errors. */
    if (ex instanceof JsonMappingException) {
      return MyriaExceptionMapper.getResponse(Status.BAD_REQUEST, ex);
    }
    /** EOFException is thrown by Jackson on null input. */
    if (ex instanceof EOFException
        && ex.getMessage().equals("No content to map to Object due to end of input")) {
      return MyriaExceptionMapper.getResponse(Status.BAD_REQUEST, "JSON payload cannot be empty");
    }

    LOGGER.info("Returning exception of type {} as 500 INTERNAL_SERVER_ERROR", ex.getClass());
    return getResponse(Status.INTERNAL_SERVER_ERROR, ex);
  }

  /**
   * Get the HTTP response for this status and explanation.
   *
   * @param status the HTTP status code of the response.
   * @param explanation the explanation of the HTTP error.
   * @return the Response.
   */
  public static Response getResponse(final Status status, final String explanation) {
    Response ret = Response.status(status).entity(explanation).type(MediaType.TEXT_PLAIN).build();
    LOGGER.trace("In getResponse::Status:String");
    return ret;
  }

  /**
   * Get the HTTP response for this status and cause. If the Status code is 500 (Internal Server Error) then we return
   * the entire stack trace. Otherwise, we just return the message.
   *
   * @param status the HTTP status code to be returned.
   * @param cause the reason for the exception.
   * @return the HTTP Response.
   */
  public static Response getResponse(final Status status, final Throwable cause) {
    /* Pass MyriaApiException objects right on through. */
    if (cause instanceof MyriaApiException) {
      LOGGER.trace("Here in getResponse::Status:Throwable");
      return ((MyriaApiException) cause).getResponse();
    }
    ResponseBuilder ret = Response.status(status);
    if (status.equals(Status.INTERNAL_SERVER_ERROR)
        || cause.getMessage() == null
        || cause.getMessage().length() == 0) {
      ret.entity(ErrorUtils.getStackTrace(cause));
    } else {
      ret.entity(cause.getMessage());
    }
    LOGGER.trace("In getResponse::Status:Throwable");
    return ret.type(MediaType.TEXT_PLAIN).build();
  }
}
