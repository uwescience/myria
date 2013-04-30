package edu.washington.escience.myriad.api;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.restlet.Context;

import edu.washington.escience.myriad.parallel.Server;

/**
 * Helper functions to simplify Restlet representation-writing code.
 * 
 * @author dhalperi
 * 
 */
public final class MyriaApiUtils {
  /** Utility class, do not construct. */
  private MyriaApiUtils() {
  }

  /** Used to deserialize JSON objects. */
  private static ThreadLocal<ObjectMapper> mapper = new ThreadLocal<ObjectMapper>();

  /**
   * @return the set of attributes in the Restlet Context.
   */
  public static Map<String, Object> getContextAttributes() {
    return Context.getCurrent().getAttributes();
  }

  /**
   * @return the Myria Server in the Restlet Context.
   */
  public static Server getServer() {
    return (Server) getContextAttributes().get(MyriaApiConstants.MYRIA_SERVER_ATTRIBUTE);
  }

  /**
   * @return an ObjectMapper to deserialize objects.
   */
  private static ObjectMapper getMapper() {
    ObjectMapper m = mapper.get();
    if (m == null) {
      m = new ObjectMapper();
      m.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
      mapper.set(m);
      return m;
    }
    return m;
  }

  /**
   * Deserialize a JSON object of type target.
   * 
   * @param <T> the type of the returned object
   * @param payload the bytes of the request.
   * @param target the class of the object to be returned.
   * @return the deserialized object.
   */
  public static <T> T deserialize(final byte[] payload, final Class<? extends T> target) {
    try {
      return getMapper().readValue(payload, target);
    } catch (IOException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }
  }
}
