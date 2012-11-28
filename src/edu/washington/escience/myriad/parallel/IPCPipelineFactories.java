package edu.washington.escience.myriad.parallel;

import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.TransportProto;

public class IPCPipelineFactories {

  final protected static ProtobufEncoder protobufEncoder = new ProtobufEncoder();
  final protected static ProtobufVarint32LengthFieldPrepender frameEncoder = new ProtobufVarint32LengthFieldPrepender();
  final protected static ProtobufDecoder protobufDecoder = new ProtobufDecoder(TransportProto.TransportMessage
      .getDefaultInstance());
  final protected static IPCInputGuard inputVerifier = new IPCInputGuard();
  final protected static MasterControlHandler masterControlHandler = new MasterControlHandler();

  public static class WorkerServerPipelineFactory implements ChannelPipelineFactory {

    /**
     * constructor.
     * */
    private WorkerServerPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageBuffer) {
      workerDataHandler = new WorkerDataHandler(messageBuffer);
    }

    private static WorkerServerPipelineFactory instance;
    private final WorkerDataHandler workerDataHandler;

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline p = Channels.pipeline();
      p.addLast("compressionDecoder", new ZlibDecoder()); // upstream 1
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", protobufDecoder); // upstream
                                                     // 3

      p.addLast("compressionEncoder", new ZlibEncoder()); // downstream 1
      p.addLast("frameEncoder", frameEncoder); // downstream 2
      p.addLast("protobufEncoder", protobufEncoder); // downstream 3

      p.addLast("inputVerifier", inputVerifier);
      p.addLast("controlHandler", workerControlHandler);
      p.addLast("dataHandler", workerDataHandler);
      return p;
    }

    static synchronized WorkerServerPipelineFactory getInstance(LinkedBlockingQueue<MessageWrapper> messageBuffer) {
      if (instance == null) {
        instance = new WorkerServerPipelineFactory(messageBuffer);
      } else if (messageBuffer != instance.workerDataHandler.messageBuffer) {
        // A new server instance
        instance = new WorkerServerPipelineFactory(messageBuffer);
      }
      return instance;
    }

    protected static WorkerControlHandler workerControlHandler = new WorkerControlHandler();
  }

  public static class WorkerClientPipelineFactory implements ChannelPipelineFactory {
    /**
     * constructor.
     * */
    private WorkerClientPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageBuffer) {
      this.messageBuffer = messageBuffer;
    }

    final LinkedBlockingQueue<MessageWrapper> messageBuffer;
    static WorkerClientPipelineFactory instance = null;

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      return new WorkerServerPipelineFactory(messageBuffer).getPipeline();
    }

    static synchronized WorkerClientPipelineFactory getInstance(LinkedBlockingQueue<MessageWrapper> messageBuffer) {
      if (instance == null) {
        instance = new WorkerClientPipelineFactory(messageBuffer);
      } else if (messageBuffer != instance.messageBuffer) {
        // A new server instance
        instance = new WorkerClientPipelineFactory(messageBuffer);
      }
      return instance;
    }
  }

  public static class MasterServerPipelineFactory implements ChannelPipelineFactory {

    /**
     * constructor.
     * */
    private MasterServerPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageBuffer) {
      masterDataHandler = new MasterDataHandler(messageBuffer);
    }

    final protected MasterDataHandler masterDataHandler;
    private static MasterServerPipelineFactory instance;

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline p = Channels.pipeline();
      p.addLast("compressionDecoder", new ZlibDecoder()); // upstream 1
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", protobufDecoder); // upstream
                                                     // 3

      p.addLast("compressionEncoder", new ZlibEncoder()); // downstream 1
      p.addLast("frameEncoder", frameEncoder); // downstream 2
      p.addLast("protobufEncoder", protobufEncoder); // downstream 3

      p.addLast("inputVerifier", inputVerifier);
      p.addLast("controlHandler", masterControlHandler);
      p.addLast("dataHandler", masterDataHandler);
      return p;
    }

    static synchronized MasterServerPipelineFactory getInstance(LinkedBlockingQueue<MessageWrapper> messageBuffer) {
      if (instance == null) {
        instance = new MasterServerPipelineFactory(messageBuffer);
      } else if (messageBuffer != instance.masterDataHandler.messageBuffer) {
        // A new server instance
        instance = new MasterServerPipelineFactory(messageBuffer);
      }
      return instance;
    }

  }

  public static class MasterClientPipelineFactory implements ChannelPipelineFactory {

    /**
     * constructor.
     * */
    private MasterClientPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageBuffer) {
      this.messageBuffer = messageBuffer;
    }

    static synchronized MasterClientPipelineFactory getInstance(LinkedBlockingQueue<MessageWrapper> messageBuffer) {
      if (instance == null) {
        instance = new MasterClientPipelineFactory(messageBuffer);
      } else if (messageBuffer != instance.messageBuffer) {
        // A new server instance
        instance = new MasterClientPipelineFactory(messageBuffer);
      }
      return instance;
    }

    private final LinkedBlockingQueue<MessageWrapper> messageBuffer;
    private static MasterClientPipelineFactory instance;

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      return new MasterServerPipelineFactory(messageBuffer).getPipeline();
    }
  }
}
