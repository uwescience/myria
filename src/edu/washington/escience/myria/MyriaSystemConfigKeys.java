package edu.washington.escience.myria;

import java.util.Map;

import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig;

import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.StreamInputBuffer;

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
  public static final String OPERATOR_INPUT_BUFFER_CAPACITY = "operator.consumer.inputbuffer.capacity";

  /**
   * After an input buffer full event, if the size of the input buffer reduced to the recover_trigger, the input buffer
   * recover event should be issued.
   * */
  public static final String OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER = "operator.consumer.inputbuffer.recover.trigger";

  /**
   * .
   * */
  public static final String TCP_SEND_BUFFER_SIZE_BYTES = "tcp.sendbuffer.size.bytes";

  /**
   * .
   * */
  public static final String TCP_RECEIVE_BUFFER_SIZE_BYTES = "tcp.receivebuffer.size.bytes";

  /**
   * See {@link NioSocketChannelConfig#setWriteBufferLowWaterMark}.
   * */
  public static final String FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES = "flowcontrol.writebuffer.watermark.low";

  /**
   * See {@link NioSocketChannelConfig#setWriteBufferHighWaterMark}.
   * */
  public static final String FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES = "flowcontrol.writebuffer.watermark.high";

  /**
   * TCP timeout.
   * */
  public static final String TCP_CONNECTION_TIMEOUT_MILLIS = "tcp.connection.timeout.milliseconds";

  /**
   * .
   * */
  public static final String WORKER_STORAGE_DATABASE_SYSTEM = "worker.storage.database.type";

  /**
   * .
   * */
  public static final String WORKER_STORAGE_DATABASE_CONN_INFO = "worker.storage.database.conn.info";

  /**
   * .
   * */
  public static final String WORKER_IDENTIFIER = "worker.identifier";

  /** */
  public static final String WORKING_DIRECTORY = "working.directory";
  /** */
  public static final String DESCRIPTION = "description";
  /** */
  public static final String USERNAME = "username";
  /** */
  public static final String MAX_HEAP_SIZE = "max.heap.size";
  /** */
  public static final String DEPLOYMENT_FILE = "deployment.file";
  /** */
  public static final String ADMIN_PASSWORD = "admin.password";

  /**
   * Add default configurations into a configuraion.
   * 
   * @param config the configuration.
   * */
  public static void addDefaultConfigKeys(final Map<String, String> config) {
    if (!config.containsKey(FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)
        || config.get(FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES) == null) {
      config.put(FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES,
          MyriaConstants.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)
        || config.get(FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES) == null) {
      config.put(FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES,
          MyriaConstants.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(OPERATOR_INPUT_BUFFER_CAPACITY) || config.get(OPERATOR_INPUT_BUFFER_CAPACITY) == null) {
      config.put(OPERATOR_INPUT_BUFFER_CAPACITY, MyriaConstants.OPERATOR_INPUT_BUFFER_CAPACITY_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER)
        || config.get(OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER) == null) {
      config.put(OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER,
          MyriaConstants.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(TCP_CONNECTION_TIMEOUT_MILLIS) || config.get(TCP_CONNECTION_TIMEOUT_MILLIS) == null) {
      config.put(TCP_CONNECTION_TIMEOUT_MILLIS, MyriaConstants.TCP_CONNECTION_TIMEOUT_MILLIS_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(TCP_RECEIVE_BUFFER_SIZE_BYTES) || config.get(TCP_RECEIVE_BUFFER_SIZE_BYTES) == null) {
      config.put(TCP_RECEIVE_BUFFER_SIZE_BYTES, MyriaConstants.TCP_RECEIVE_BUFFER_SIZE_BYTES_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(TCP_SEND_BUFFER_SIZE_BYTES) || config.get(TCP_SEND_BUFFER_SIZE_BYTES) == null) {
      config.put(TCP_SEND_BUFFER_SIZE_BYTES, MyriaConstants.TCP_SEND_BUFFER_SIZE_BYTES_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(WORKER_STORAGE_DATABASE_SYSTEM) || config.get(WORKER_STORAGE_DATABASE_SYSTEM) == null) {
      config.put(WORKER_STORAGE_DATABASE_SYSTEM, MyriaConstants.WORKER_STORAGE_DATABASE_SYSTEM_DEFAULT_VALUE + "");
    }
  }

  /**
   * Add configurations from the parsed result of the deployment section of a config file.
   * 
   * @param config the configuration to be added to.
   * @param deployment the parsed result of the deployment section.
   * */
  public static void addDeploymentKeysFromConfigFile(final Map<String, String> config,
      final Map<String, String> deployment) {
    if (!config.containsKey(WORKING_DIRECTORY) || config.get(WORKING_DIRECTORY) == null) {
      config.put(WORKING_DIRECTORY, deployment.get("path"));
    }
    if (!config.containsKey(DESCRIPTION) || config.get(DESCRIPTION) == null) {
      config.put(DESCRIPTION, deployment.get("name"));
    }
    if (!config.containsKey(USERNAME) || config.get(USERNAME) == null) {
      config.put(USERNAME, deployment.get("username"));
    }
    if (!config.containsKey(MAX_HEAP_SIZE) || config.get(MAX_HEAP_SIZE) == null) {
      config.put(MAX_HEAP_SIZE, deployment.get("max_heap_size"));
    }
    if (!config.containsKey(WORKER_STORAGE_DATABASE_SYSTEM) || config.get(WORKER_STORAGE_DATABASE_SYSTEM) == null) {
      config.put(WORKER_STORAGE_DATABASE_SYSTEM, deployment.get("dbms"));
    }
    if (!config.containsKey(ADMIN_PASSWORD) || config.get(ADMIN_PASSWORD) == null) {
      config.put(ADMIN_PASSWORD, deployment.get("admin_password"));
    }
  }

}
