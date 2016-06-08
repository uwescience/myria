package edu.washington.escience.myria.api;

/**
 * This class holds constants used in the Myria API server.
 *
 */
public final class MyriaApiConstants {
  /** This class just holds constants. */
  private MyriaApiConstants() {}

  /** The Myria Server in the Restlet Context variable. */
  public static final String MYRIA_SERVER_ATTRIBUTE = "myria.server";
  /** The Myria MasterDaemon in the Restlet Context variable. */
  public static final String MYRIA_MASTER_DAEMON_ATTRIBUTE = "myria.master_daemon";
  /** The Myria ApiServer in the Restlet Context variable. */
  public static final String MYRIA_API_SERVER_ATTRIBUTE = "myria.master_api_server";
  /** The path to the SSL Keystore. */
  public static final String MYRIA_API_SSL_KEYSTORE = "myria.master_api_server.ssl.keystore_path";
  /** The password to the SSL Keystore. */
  public static final String MYRIA_API_SSL_KEYSTORE_PASSWORD =
      "myria.master_api_server.ssl.keystore_password";
  /** JSON, UTF-8 is the default response type when serializing JSON. */
  public static final String JSON_UTF_8 = "application/json; charset=UTF-8";

  /** The default number of results returned in a large query. */
  public static final Long MYRIA_API_DEFAULT_NUM_RESULTS = 10L;
}
