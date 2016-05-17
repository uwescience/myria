package edu.washington.escience.myria.api;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.slf4j.LoggerFactory;

/**
 * A helper function to easily create HTTP error responses in the API server.
 *
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
    super(MyriaExceptionMapper.getResponse(status, cause));
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
    super(new RuntimeException(explanation), MyriaExceptionMapper.getResponse(status, explanation));
    LOGGER.trace("In Status:String (log will be after getResponse)");
  }
}
