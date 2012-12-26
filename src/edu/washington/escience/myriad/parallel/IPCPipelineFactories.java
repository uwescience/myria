package edu.washington.escience.myriad.parallel;

import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
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
    WorkerServerPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageQueue) {
      workerDataHandler = new WorkerDataHandler(messageQueue);
    }

    private final WorkerDataHandler workerDataHandler;

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline p = Channels.pipeline();
      // p.addLast("compressionDecoder", new ZlibDecoder()); // upstream 1
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", protobufDecoder); // upstream
                                                     // 3

      // p.addLast("compressionEncoder", new ZlibEncoder(1)); // downstream 1
      p.addLast("frameEncoder", frameEncoder); // downstream 2
      p.addLast("protobufEncoder", protobufEncoder); // downstream 3

      p.addLast("inputVerifier", inputVerifier);
      p.addLast("controlHandler", workerControlHandler);
      p.addLast("dataHandler", workerDataHandler);
      return p;
    }

    protected static WorkerControlHandler workerControlHandler = new WorkerControlHandler();
  }

  public static class WorkerClientPipelineFactory implements ChannelPipelineFactory {
    /**
     * constructor.
     * */
    WorkerClientPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageQueue) {
      this.messageQueue = messageQueue;
    }

    final LinkedBlockingQueue<MessageWrapper> messageQueue;

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      return new WorkerServerPipelineFactory(messageQueue).getPipeline();
    }

  }

  public static class MasterServerPipelineFactory implements ChannelPipelineFactory {

    /**
     * constructor.
     * */
    MasterServerPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageQueue) {
      masterDataHandler = new MasterDataHandler(messageQueue);
    }

    final protected MasterDataHandler masterDataHandler;

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline p = Channels.pipeline();
      // p.addLast("compressionDecoder", new ZlibDecoder()); // upstream 1
      p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); // upstream 2
      p.addLast("protobufDecoder", protobufDecoder); // upstream
                                                     // 3

      // p.addLast("compressionEncoder", new ZlibEncoder(1)); // downstream 1
      p.addLast("frameEncoder", frameEncoder); // downstream 2
      p.addLast("protobufEncoder", protobufEncoder); // downstream 3

      p.addLast("inputVerifier", inputVerifier);
      p.addLast("controlHandler", masterControlHandler);
      p.addLast("dataHandler", masterDataHandler);
      return p;
    }

  }

  public static class MasterClientPipelineFactory implements ChannelPipelineFactory {

    /**
     * constructor.
     * */
    MasterClientPipelineFactory(final LinkedBlockingQueue<MessageWrapper> messageQueue) {
      this.messageQueue = messageQueue;
    }

    private final LinkedBlockingQueue<MessageWrapper> messageQueue;

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      return new MasterServerPipelineFactory(messageQueue).getPipeline();
    }
  }
}
