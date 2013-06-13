package edu.washington.escience.myriad.api;

import java.io.IOException;

import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;

import edu.washington.escience.myriad.api.encoding.MyriaApiEncoding;

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
    if (payload == null || payload.length == 0) {
      throw new MyriaApiException(Status.BAD_REQUEST, "payload cannot be empty");
    }
    try {
      T t = getMapper().readValue(payload, target);
      if (t instanceof MyriaApiEncoding) {
        ((MyriaApiEncoding) t).validate();
      }
      return t;
    } catch (IOException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }
  }
}
