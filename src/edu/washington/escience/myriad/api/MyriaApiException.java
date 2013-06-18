package edu.washington.escience.myriad.api;

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.slf4j.LoggerFactory;

/**
 * A helper function to easily create HTTP error responses in the API server.
 * 
 * @author dhalperi
 * 
 */
public final class MyriaApiException extends WebApplicationException {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MyriaApiException.class);

  /**
   * Construct a MyriaApiException from the given status and cause. The entity of the HTTP Response includes the given
   * HTTP status code and the body is the exception cause. If the status code is 500 INTERNAL SERVER ERROR then we
   * return the entire stack trace. Otherwise, we just return the message.
   * 
   * @param status the HTTP status code used for the HTTP response.
   * @param cause the Exception, whose message is used to explain the exception in the HTTP response.
   */
  public MyriaApiException(final Status status, final Throwable cause) {
    super(getResponse(status, cause));
    LOGGER.trace("In Status:Throwable (log will be after getResponse)");
  }

  /**
   * Construct a MyriaApiException from the given status and explanation. The entity of the HTTP Response includes the
   * given HTTP status code and the body is the provided explanation string.
   * 
   * @param status the HTTP status code used for the HTTP response.
   * @param explanation the message is used to explain the exception in the HTTP response.
   */
  public MyriaApiException(final Status status, final String explanation) {
    super(getResponse(status, explanation));
    LOGGER.trace("In Status:String (log will be after getResponse)");
  }

  /**
   * Get the HTTP response for this status and cause. If the Status code is 500 (Internal Server Error) then we return
   * the entire stack trace. Otherwise, we just return the message.
   * 
   * @param status the HTTP status code to be returned.
   * @param cause the reason for the exception.
   * @return the HTTP Response.
   */
  private static Response getResponse(final Status status, final Throwable cause) {
    /* Pass MyriaApiException objects right on through. */
    if (cause instanceof MyriaApiException) {
      LOGGER.trace("Here in getResponse::Status:Throwable");
      return ((MyriaApiException) cause).getResponse();
    }
    ResponseBuilder ret = Response.status(status);
    if (status.equals(Status.INTERNAL_SERVER_ERROR) || cause.getMessage() == null || cause.getMessage().length() == 0) {
      StringWriter stringWriter = new StringWriter();
      cause.printStackTrace(new PrintWriter(stringWriter));
      String stackTrace = stringWriter.toString();
      ret.entity(stackTrace);
    } else {
      ret.entity(cause.getMessage());
    }
    LOGGER.trace("In getResponse::Status:Throwable");
    return ret.build();
  }

  /**
   * Get the HTTP response for this status and explanation.
   * 
   * @param status the HTTP status code of the response.
   * @param explanation the explanation of the HTTP error.
   * @return the Response.
   */
  private static Response getResponse(final Status status, final String explanation) {
    Response ret = Response.status(status).entity(explanation).build();
    LOGGER.trace("In getResponse::Status:String");
    return ret;
  }
}
