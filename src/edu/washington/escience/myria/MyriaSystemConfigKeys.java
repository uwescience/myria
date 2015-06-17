package edu.washington.escience.myria;

import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig;

import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.StreamInputBuffer;
import edu.washington.escience.myria.tools.MyriaConfiguration;

/**
 * Myria system configuration keys.
 * */
public final class MyriaSystemConfigKeys {

  /**
   * .
   * */
  private MyriaSystemConfigKeys() {
  }

  /**
   * The max number of data messages that the {@link StreamInputBuffer} of each {@link Consumer} operator should hold.
   * It's not a restrict upper bound. Different implementations of {@link StreamInputBuffer} may restrict the size
   * differently. For example, a {@link FlowControlBagInputBuffer} use the upper bound as a soft restriction.
   * */
  public static final String OPERATOR_INPUT_BUFFER_CAPACITY = "operator_consumer_inputbuffer_capacity";

  /**
   * After an input buffer full event, if the size of the input buffer reduced to the recover_trigger, the input buffer
   * recover event should be issued.
   * */
  public static final String OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER = "operator_consumer_inputbuffer_recover_trigger";

  /**
   * .
   * */
  public static final String TCP_SEND_BUFFER_SIZE_BYTES = "tcp_sendbuffer_size_bytes";

  /**
   * .
   * */
  public static final String TCP_RECEIVE_BUFFER_SIZE_BYTES = "tcp_receivebuffer_size_bytes";

  /**
   * See {@link NioSocketChannelConfig#setWriteBufferLowWaterMark}.
   * */
  public static final String FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES = "flowcontrol_writebuffer_watermark_low";

  /**
   * See {@link NioSocketChannelConfig#setWriteBufferHighWaterMark}.
   * */
  public static final String FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES = "flowcontrol_writebuffer_watermark_high";

  /**
   * TCP timeout.
   * */
  public static final String TCP_CONNECTION_TIMEOUT_MILLIS = "tcp_connection_timeout_milliseconds";

  /** */
  public static final String WORKER_STORAGE_DATABASE_SYSTEM = "dbms";

  /** */
  public static final String WORKER_STORAGE_DATABASE_NAME = "database_name";

  /** */
  public static final String WORKER_STORAGE_DATABASE_PASSWORD = "database_password";

  /** */
  public static final String WORKER_STORAGE_DATABASE_PORT = "database_port";

  /** */
  public static final String WORKER_IDENTIFIER = "worker_id";
  /** */
  public static final String DEPLOYMENT_PATH = "path";
  /** */
  public static final String DESCRIPTION = "name";
  /** */
  public static final String USERNAME = "username";
  /** */
  public static final String MAX_HEAP_SIZE_GB = "max_heap_size_gb";
  /** */
  public static final String MIN_HEAP_SIZE_GB = "min_heap_size_gb";
  /** */
  public static final String DEPLOYMENT_CONF_FILE = "deployment.cfg";
  /** */
  public static final String ADMIN_PASSWORD = "admin_password";
  /** */
  public static final String REST_PORT = "rest_port";
  /** */
  public static final String SSL = "ssl";
  /** */
  public static final String DEBUG = "DEBUG";

  /**
   * Default value for {@link MyriaSystemConfigKeys#FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES}.
   */
  public static final int FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES_DEFAULT_VALUE = 5 * MyriaConstants.MB;

  /**
   * Default value for {@link MyriaSystemConfigKeys#FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES}.
   */
  public static final int FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES_DEFAULT_VALUE = 512 * MyriaConstants.KB;

  /**
   * Default value for {@link MyriaSystemConfigKeys#OPERATOR_INPUT_BUFFER_CAPACITY}.
   */
  public static final int OPERATOR_INPUT_BUFFER_CAPACITY_DEFAULT_VALUE = 100;

  /**
   * Default value for {@link MyriaSystemConfigKeys#OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER}.
   */
  public static final int OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER_DEFAULT_VALUE = 80;

  /**
   * Default value for {@link MyriaSystemConfigKeys#TCP_CONNECTION_TIMEOUT_MILLIS}.
   */
  public static final int TCP_CONNECTION_TIMEOUT_MILLIS_DEFAULT_VALUE = 3000;

  /**
   * Default value for {@link MyriaSystemConfigKeys#TCP_RECEIVE_BUFFER_SIZE_BYTES}.
   */
  public static final int TCP_RECEIVE_BUFFER_SIZE_BYTES_DEFAULT_VALUE = 2 * MyriaConstants.MB;

  /**
   * Default value for {@link MyriaSystemConfigKeys#TCP_SEND_BUFFER_SIZE_BYTES}.
   */
  public static final int TCP_SEND_BUFFER_SIZE_BYTES_DEFAULT_VALUE = 5 * MyriaConstants.MB;

  /**
   * Default value for {@link MyriaSystemConfigKeys#WORKER_STORAGE_DATABASE_SYSTEM}.
   */
  public static final String WORKER_STORAGE_DATABASE_SYSTEM_DEFAULT_VALUE = MyriaConstants.STORAGE_SYSTEM_SQLITE;

  /**
   * 
   * @param config the configuration.
   * @param section the section.
   * @param key the key.
   * @param defaultValue if the key is not in the config file, set its value to be the provided default value.
   */
  private static void setDefaultValueIfMissing(final MyriaConfiguration config, final String section, final String key,
      final String defaultValue) {
    if (config.getOptional(section, key) == null) {
      config.setValue(section, key, defaultValue);
    }
  }

  /**
   * Add default configurations into a configuration.
   * 
   * @param config the configuration.
   * */
  public static void addDefaultConfigValues(final MyriaConfiguration config) {
    setDefaultValueIfMissing(config, "deployment", WORKER_STORAGE_DATABASE_SYSTEM,
        WORKER_STORAGE_DATABASE_SYSTEM_DEFAULT_VALUE);
    setDefaultValueIfMissing(config, "runtime", FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES,
        FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES_DEFAULT_VALUE + "");
    setDefaultValueIfMissing(config, "runtime", FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES,
        FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES_DEFAULT_VALUE + "");
    setDefaultValueIfMissing(config, "runtime", OPERATOR_INPUT_BUFFER_CAPACITY,
        OPERATOR_INPUT_BUFFER_CAPACITY_DEFAULT_VALUE + "");
    setDefaultValueIfMissing(config, "runtime", OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER,
        OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER_DEFAULT_VALUE + "");
    setDefaultValueIfMissing(config, "runtime", TCP_CONNECTION_TIMEOUT_MILLIS,
        TCP_CONNECTION_TIMEOUT_MILLIS_DEFAULT_VALUE + "");
    setDefaultValueIfMissing(config, "runtime", TCP_RECEIVE_BUFFER_SIZE_BYTES,
        TCP_RECEIVE_BUFFER_SIZE_BYTES_DEFAULT_VALUE + "");
    setDefaultValueIfMissing(config, "runtime", TCP_SEND_BUFFER_SIZE_BYTES, TCP_SEND_BUFFER_SIZE_BYTES_DEFAULT_VALUE
        + "");
  }
}
