package edu.washington.escience.myria;

import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig;

import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.StreamInputBuffer;

/**
 * Myria system configuration keys.
 */
public final class MyriaSystemConfigKeys {

  /**
   * This is a purely static class.
   */
  private MyriaSystemConfigKeys() {}

  /**
   * The max number of data messages that the {@link StreamInputBuffer} of each {@link Consumer} operator should hold.
   * It's not a restrict upper bound. Different implementations of {@link StreamInputBuffer} may restrict the size
   * differently. For example, a {@link FlowControlBagInputBuffer} use the upper bound as a soft restriction.
   */
  public static final String OPERATOR_INPUT_BUFFER_CAPACITY =
      "operator.consumer.inputbuffer.capacity";

  /**
   * After an input buffer full event, if the size of the input buffer reduced to the recover_trigger, the input buffer
   * recover event should be issued.
   */
  public static final String OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER =
      "operator.consumer.inputbuffer.recover.trigger";

  public static final String TCP_SEND_BUFFER_SIZE_BYTES = "tcp.sendbuffer.size.bytes";

  public static final String TCP_RECEIVE_BUFFER_SIZE_BYTES = "tcp.receivebuffer.size.bytes";

  /**
   * See {@link NioSocketChannelConfig#setWriteBufferLowWaterMark}.
   */
  public static final String FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES =
      "flowcontrol.writebuffer.watermark.low";

  /**
   * See {@link NioSocketChannelConfig#setWriteBufferHighWaterMark}.
   */
  public static final String FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES =
      "flowcontrol.writebuffer.watermark.high";

  public static final String TCP_CONNECTION_TIMEOUT_MILLIS = "tcp.connection.timeout.milliseconds";

  public static final String WORKER_STORAGE_DATABASE_SYSTEM = "dbms";

  public static final String WORKER_STORAGE_DATABASE_NAME = "database_name";

  public static final String WORKER_STORAGE_DATABASE_PASSWORD = "database_password";

  public static final String WORKER_STORAGE_DATABASE_PORT = "database_port";

  public static final String WORKER_IDENTIFIER = "worker_id";

  public static final String DEPLOYMENT_PATH = "path";

  public static final String DESCRIPTION = "name";

  public static final String USERNAME = "username";

  public static final String MASTER_NUMBER_VCORES = "container.master.vcores.number";

  public static final String WORKER_NUMBER_VCORES = "container.worker.vcores.number";

  public static final String DRIVER_MEMORY_QUOTA_GB = "container.driver.memory.size.gb";

  public static final String MASTER_MEMORY_QUOTA_GB = "container.master.memory.size.gb";

  public static final String WORKER_MEMORY_QUOTA_GB = "container.worker.memory.size.gb";

  public static final String MASTER_JVM_HEAP_SIZE_MAX_GB = "jvm.master.heap.size.max.gb";

  public static final String WORKER_JVM_HEAP_SIZE_MAX_GB = "jvm.worker.heap.size.max.gb";

  public static final String MASTER_JVM_HEAP_SIZE_MIN_GB = "jvm.master.heap.size.min.gb";

  public static final String WORKER_JVM_HEAP_SIZE_MIN_GB = "jvm.worker.heap.size.min.gb";

  public static final String DEPLOYMENT_CONF_FILE = "deployment.cfg";

  public static final String ADMIN_PASSWORD = "admin_password";

  public static final String REST_PORT = "rest_port";

  public static final String SSL = "ssl";

  public static final String DEBUG = "debug";

  public static final String JVM_OPTIONS = "jvm.options";

  public static final String GANGLIA_MASTER_HOST = "ganglia.master.host";

  public static final String GANGLIA_MASTER_PORT = "ganglia.master.port";

  public static final String PERSIST_URI = "persist_uri";

  public static final String ELASTIC_MODE = "elastic_mode";
}
