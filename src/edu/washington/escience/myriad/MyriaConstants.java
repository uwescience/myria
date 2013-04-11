package edu.washington.escience.myriad;

/**
 * This class holds the constants for the Myria execution.
 * 
 * @author dhalperi
 * 
 */
public final class MyriaConstants {
  /** Private constructor to disallow building utility class. */
  private MyriaConstants() {
  }

  /** The identity of the master worker is current always zero. */
  public static final int MASTER_ID = 0;

  /**
   * The system name.
   * */
  public static final String SYSTEM_NAME = "Myria";

  /**
   * Short wait interval in milliseconds.
   * */
  public static final int SHORT_WAITING_INTERVAL_MS = 100;

  /**
   * Short wait interval in milliseconds.
   * */
  public static final int WAITING_INTERVAL_1_SECOND_IN_MS = 1000;

  /**
   * 1 kb.
   * */
  public static final int KB = 1024;

  /**
   * 1 mb.
   * */
  public static final int MB = 1024 * 1024;

  /**
   * 1 gb.
   * */
  public static final int GB = 1024 * 1024 * 1024;

  /**
   * 1 tb.
   * */
  public static final int TB = 1024 * 1024 * 1024 * 1024;

  /**
   * Sqlite storage.
   * */
  public static final String STORAGE_SYSTEM_SQLITE = "sqlite";

  /**
   * Mysql storage.
   * */
  public static final String STORAGE_SYSTEM_MYSQL = "mysql";

  public static final String EXEC_ENV_VAR_SQLITE_FILE = "sqliteFile";

  /**
   * Monetdb storage.
   * */
  public static final String STORAGE_SYSTEM_MONETDB = "monetdb";

  /**
   * Default value for {@link MyriaSystemConfigKeys#FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES}.
   * */
  public static final int FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES_DEFAULT_VALUE = 512 * MyriaConstants.KB;

  /**
   * Default value for {@link MyriaSystemConfigKeys#OPERATOR_INPUT_BUFFER_CAPACITY}.
   * */
  public static final int OPERATOR_INPUT_BUFFER_CAPACITY_DEFAULT_VALUE = 100;

  /**
   * Default value for {@link MyriaSystemConfigKeys#TCP_SEND_BUFFER_SIZE_BYTES}.
   * */
  public static final int TCP_SEND_BUFFER_SIZE_BYTES_DEFAULT_VALUE = 5 * MyriaConstants.MB;

  /**
   * Default value for {@link MyriaSystemConfigKeys#TCP_RECEIVE_BUFFER_SIZE_BYTES}.
   * */
  public static final int TCP_RECEIVE_BUFFER_SIZE_BYTES_DEFAULT_VALUE = 2 * MyriaConstants.MB;

  /**
   * Default value for {@link MyriaSystemConfigKeys#FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES}.
   * */
  public static final int FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES_DEFAULT_VALUE = 5 * MyriaConstants.MB;

  /**
   * Default value for {@link MyriaSystemConfigKeys#TCP_CONNECTION_TIMEOUT_MILLIS}.
   * */
  public static final int TCP_CONNECTION_TIMEOUT_MILLIS_DEFAULT_VALUE = 3000;

  /**
   * If a thread in a thread pool is idle, how long it should wait before terminates itself. Currently, 5 minutes.
   */
  public static final int THREAD_POOL_KEEP_ALIVE_TIME_IN_MS = 1000 * 60 * 5;

}
