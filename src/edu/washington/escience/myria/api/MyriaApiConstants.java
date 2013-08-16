package edu.washington.escience.myria.api;

/**
 * This class holds constants used in the Myria API server.
 * 
 * @author dhalperi
 * 
 */
public final class MyriaApiConstants {
  /** This class just holds constants. */
  private MyriaApiConstants() {
  }

  /** The string identifying the Myria Server in the Restlet Context variable. */
  public static final String MYRIA_SERVER_ATTRIBUTE = "myria.server";
  /** The string identifying the Myria MasterDaemon in the Restlet Context variable. */
  public static final String MYRIA_MASTER_DAEMON_ATTRIBUTE = "myria.master_daemon";
  /** The string identifying the Myria ApiServer in the Restlet Context variable. */
  public static final String MYRIA_API_SERVER_ATTRIBUTE = "myria.master_api_server";
}
