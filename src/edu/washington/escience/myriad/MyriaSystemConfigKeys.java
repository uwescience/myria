package edu.washington.escience.myriad;

import java.util.Map;

import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig;

import edu.washington.escience.myriad.parallel.Consumer;
import edu.washington.escience.myriad.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myriad.parallel.ipc.StreamInputBuffer;

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
  public static final String WORKER_STORAGE_SYSTEM_TYPE = "worker.storage.system";

  /**
   * .
   * */
  public static final String WORKER_IDENTIFIER = "worker.identifier";

  /**
   * .
   * */
  public static final String WORKER_DATA_SQLITE_DB = "worker.data.sqlite.db";

  /**
   * .
   * */
  public static final String IPC_SERVER_HOSTNAME = "ipc.server.hostname";

  /**
   * .
   * */
  public static final String IPC_SERVER_PORT = "ipc.server.port";

  /** */
  public static final String WORKING_DIRECTORY = "working.directory";
  /** */
  public static final String DESCRIPTION = "description";
  /** */
  public static final String USERNAME = "username";
  /** */
  public static final String MAX_HEAP_SIZE = "max.heap.size";
  /** */
  public static final String CONFIG_FILE = "configuration.file";

  /**
   * Add default configurations into a configuraion.
   * 
   * @param config the configuration.
   * */
  public static void addDefaultConfigKeys(final Map<String, String> config) {
    if (!config.containsKey(FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)) {
      config.put(FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES,
          MyriaConstants.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)) {
      config.put(FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES,
          MyriaConstants.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(OPERATOR_INPUT_BUFFER_CAPACITY)) {
      config.put(OPERATOR_INPUT_BUFFER_CAPACITY, MyriaConstants.OPERATOR_INPUT_BUFFER_CAPACITY_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER)) {
      config.put(OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER,
          MyriaConstants.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(TCP_CONNECTION_TIMEOUT_MILLIS)) {
      config.put(TCP_CONNECTION_TIMEOUT_MILLIS, MyriaConstants.TCP_CONNECTION_TIMEOUT_MILLIS_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(TCP_RECEIVE_BUFFER_SIZE_BYTES)) {
      config.put(TCP_RECEIVE_BUFFER_SIZE_BYTES, MyriaConstants.TCP_RECEIVE_BUFFER_SIZE_BYTES_DEFAULT_VALUE + "");
    }
    if (!config.containsKey(TCP_SEND_BUFFER_SIZE_BYTES)) {
      config.put(TCP_SEND_BUFFER_SIZE_BYTES, MyriaConstants.TCP_SEND_BUFFER_SIZE_BYTES_DEFAULT_VALUE + "");
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
    if (!config.containsKey(WORKING_DIRECTORY)) {
      config.put(WORKING_DIRECTORY, deployment.get("path"));
    }
    if (!config.containsKey(DESCRIPTION)) {
      config.put(DESCRIPTION, deployment.get("description"));
    }
    if (!config.containsKey(USERNAME)) {
      config.put(USERNAME, deployment.get("username"));
    }
    if (!config.containsKey(MAX_HEAP_SIZE)) {
      config.put(MAX_HEAP_SIZE, deployment.get("max_heap_size"));
    }
  }

}
