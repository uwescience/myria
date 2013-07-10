package edu.washington.escience.myriad;

import org.glassfish.grizzly.http.server.io.InputBuffer;
import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig;

import edu.washington.escience.myriad.parallel.Consumer;
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
   * It's not a restrict upper bound. Different implementations of {@link InputBuffer} may restrict the size
   * differently. For example, a {@link FlowControlInputBuffer} use the upper bound as a soft restriction.
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

}
