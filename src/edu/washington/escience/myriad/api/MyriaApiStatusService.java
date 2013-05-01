package edu.washington.escience.myriad.api;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Resource;
import org.restlet.service.StatusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The MyriaApiStatusService translates exceptions into error status codes. Examples error status codes are 400 BAD
 * REQUEST and 500 INTERNAL SERVER ERROR. Secondly, it populates the contents of the response when these exceptions
 * occur. In Restlet, by default, the entity is null. This class puts the entire Java stack trace in the reponse for
 * better debugging purposes.
 */
public final class MyriaApiStatusService extends StatusService {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(MyriaApiStatusService.class);

  @Override
  public Status getStatus(final Throwable throwable, final Request request, final Response respone) {
    LOGGER.debug("In getStatus(throwable,request,response)");
    return new Status(Status.SERVER_ERROR_INTERNAL, throwable);
  }

  @Override
  public Status getStatus(final Throwable throwable, final Resource resource) {
    LOGGER.debug("In getStatus(throwable,resource)");
    return new Status(Status.SERVER_ERROR_INTERNAL, throwable);
  }

  @Override
  public Representation getRepresentation(final Status status, final Request request, final Response response) {
    LOGGER.debug("In getRepresentation with status=" + status.toString());
    Throwable t = status.getThrowable();
    if (t != null) {
      StringWriter stringWriter = new StringWriter();
      t.printStackTrace(new PrintWriter(stringWriter));
      return new StringRepresentation(stringWriter.toString());
    }
    return new StringRepresentation(status.toString());
  }
}
