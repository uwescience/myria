package edu.washington.escience.myriad.api;

import java.util.Map;

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

}
