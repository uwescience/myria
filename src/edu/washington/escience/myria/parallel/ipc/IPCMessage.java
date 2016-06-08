package edu.washington.escience.myria.parallel.ipc;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.google.common.base.Preconditions;

/**
 * All IPCMessages derived from this class.
 * */
public interface IPCMessage {

  /**
   * Header of IPCMessages in serialized version.
   * */
  enum Header {
    /***/
    EOS,
    BOS,
    CONNECT,
    DISCONNECT,
    PING,
    DATA
  }

  /**
   * Meta IPCMessages, used inside the IPC module only. It has the following cases: EOS, BOS, CONNECT, DISCONNECT, PING.
   * */
  abstract class Meta implements IPCMessage {

    /**
     * EOS.
     * */
    static final Meta EOS =
        new Meta() {

          private final ChannelBuffer serializeValue =
              ChannelBuffers.wrappedBuffer(new byte[] {(byte) Header.EOS.ordinal()});

          @Override
          public ChannelBuffer serialize() {
            return serializeValue;
          }

          @Override
          public String toString() {
            return "IPCMessage.Meta.EOS";
          }
        };

    /**
     * DISCONNECT.
     * */
    static final Meta DISCONNECT =
        new Meta() {

          private final ChannelBuffer serializeValue =
              ChannelBuffers.wrappedBuffer(new byte[] {(byte) Header.DISCONNECT.ordinal()});

          @Override
          public ChannelBuffer serialize() {
            return serializeValue;
          }

          @Override
          public String toString() {
            return "IPCMessage.Meta.DISCONNECT";
          }
        };

    /**
     * BOS.
     * */
    static final class BOS extends Meta {
      /**
       * stream id.
       * */
      private final long streamID;
      /**
       * serialize value.
       * */
      private final ChannelBuffer serializeValue;

      /**
       * @param streamID stream id.
       * */
      public BOS(final long streamID) {
        this.streamID = streamID;
        ChannelBuffer bb = ChannelBuffers.buffer(1 + Long.SIZE / Byte.SIZE);
        bb.writeByte((byte) Header.BOS.ordinal());
        bb.writeLong(streamID);
        serializeValue = ChannelBuffers.unmodifiableBuffer(bb);
      }

      /**
       * @return the stream id.
       * */
      long getStreamID() {
        return streamID;
      }

      @Override
      public ChannelBuffer serialize() {
        return serializeValue.duplicate();
      }

      /**
       * @return De-serialize the BOS message.
       * @param bb serialized data.
       * */
      public static BOS deSerialize(final ChannelBuffer bb) {
        return new BOS(bb.readLong());
      }

      @Override
      public String toString() {
        return "IPCMessage.Meta.BOS(" + streamID + ")";
      }
    }

    /**
     * CONNECT.
     * */
    static final class CONNECT extends Meta {
      /**
       * remote ID.
       * */
      private final int remoteID;
      /**
       * serialize value.
       * */
      private final ChannelBuffer serializeValue;

      /**
       * @param remoteID the remote IPC ID.
       * */
      public CONNECT(final int remoteID) {
        this.remoteID = remoteID;
        ChannelBuffer bb = ChannelBuffers.buffer(1 + Integer.SIZE / Byte.SIZE);
        bb.writeByte((byte) Header.CONNECT.ordinal());
        bb.writeInt(remoteID);
        serializeValue = ChannelBuffers.unmodifiableBuffer(bb);
      }

      /**
       * @return get the remote IPC ID.
       * */
      public int getRemoteID() {
        return remoteID;
      }

      @Override
      public ChannelBuffer serialize() {
        return serializeValue.duplicate();
      }

      /**
       * @return Deserialize the CONNECT message.
       * @param bb serialized data.
       * */
      public static CONNECT deSerialize(final ChannelBuffer bb) {
        return new CONNECT(bb.readInt());
      }

      @Override
      public String toString() {
        return "IPCMessage.Meta.CONNECT(" + remoteID + ")";
      }
    }

    /**
     * PING.
     * */
    static final Meta PING =
        new Meta() {

          private final ChannelBuffer serializeValue =
              ChannelBuffers.wrappedBuffer(new byte[] {(byte) Header.PING.ordinal()});

          @Override
          public ChannelBuffer serialize() {
            return serializeValue;
          }

          @Override
          public String toString() {
            return "IPCMessage.Meta.PING";
          }
        };

    /**
     * Serialize the message.
     *
     * @return serialize result.
     * */
    public abstract ChannelBuffer serialize();

    /**
     * @return deserialize meta messages
     * @param bb serialized data.
     * */
    public static Meta deSerialize(final ChannelBuffer bb) {
      byte type = bb.readByte();
      if (type == Header.BOS.ordinal()) {
        return BOS.deSerialize(bb);
      } else if (type == Header.CONNECT.ordinal()) {
        return CONNECT.deSerialize(bb);
      } else if (type == Header.DISCONNECT.ordinal()) {
        return DISCONNECT;
      } else if (type == Header.EOS.ordinal()) {
        return EOS;
      } else if (type == Header.PING.ordinal()) {
        return PING;
      } else {
        return null;
      }
    }
  }

  /**
   * Unit of IPC transmission.
   *
   * @param <PAYLOAD> the type of payload. Currently, this PAYLOAD could only be: TransportMessage.QUERY,
   *          TransportMessage.CONTROL.
   * */
  public class Data<PAYLOAD> implements IPCMessage {

    /**
     * the payload.
     * */
    private final PAYLOAD p;
    /**
     * the source remote id.
     * */
    private final int sourceRemote;

    /**
     * @param sourceRemote the source remote id
     * @param p the payload.
     * */
    private Data(final int sourceRemote, final PAYLOAD p) {
      this.p = p;
      this.sourceRemote = sourceRemote;
    }

    /**
     * @return the payload
     * */
    public PAYLOAD getPayload() {
      return p;
    }

    /**
     * @return the source remote id.
     * */
    public int getRemoteID() {
      return sourceRemote;
    }

    /**
     * serialize head.
     * */
    static final ChannelBuffer SERIALIZE_HEAD =
        ChannelBuffers.wrappedBuffer(new byte[] {(byte) Header.DATA.ordinal()});

    /**
     * @param sourceRemote the source remote id.
     * @param maybePayload either a paylod or a Data instance.
     * @param <PAYLOAD> the payload type.
     * @return the wrapped Data message.
     * */
    @SuppressWarnings("unchecked")
    public static <PAYLOAD> Data<PAYLOAD> wrap(final int sourceRemote, final Object maybePayload) {
      if (maybePayload instanceof Data) {
        return (Data<PAYLOAD>) maybePayload;
      } else {
        return new Data<PAYLOAD>(sourceRemote, (PAYLOAD) maybePayload);
      }
    }

    @Override
    public String toString() {
      return String.format("IPCMessage.Data(from:%1$d,payload:%2$s)", sourceRemote, p);
    }
  }

  /**
   * Unit of IPC Stream.
   *
   * @param <PAYLOAD> the type of payload. Currently, this PAYLOAD could only be TupleBatch.
   * */
  public final class StreamData<PAYLOAD> extends Data<PAYLOAD> {

    /**
     * the stream ID.
     * */
    private final long streamID;

    /**
     * @param sourceRemote the source remote ID.
     * @param streamID stream ID.
     * @param p the payload.
     * */
    private StreamData(final int sourceRemote, final long streamID, final PAYLOAD p) {
      super(sourceRemote, Preconditions.checkNotNull(p));
      this.streamID = streamID;
    }

    /**
     * @param sourceRemote the source remote ID.
     * @param streamID stream ID.
     * */
    private StreamData(final int sourceRemote, final long streamID) {
      super(sourceRemote, null);
      this.streamID = streamID;
    }

    /**
     * @return the stream ID.
     * */
    public long getStreamID() {
      return streamID;
    }

    /**
     * @param <PAYLOAD> the payload type.
     * @param sourceRemote the source remote ID.
     * @param streamID stream ID.
     * @return an EOS message.
     * */
    public static <PAYLOAD> StreamData<PAYLOAD> eos(final int sourceRemote, final long streamID) {
      return new StreamData<PAYLOAD>(sourceRemote, streamID);
    }

    /**
     * @param sourceRemote the source remote id.
     * @param maybePayload either a paylod or a StreamData instance.
     * @param <PAYLOAD> the payload type.
     * @param streamID stream ID.
     * @return the wrapped StreamData message.
     * */
    @SuppressWarnings("unchecked")
    public static <PAYLOAD> StreamData<PAYLOAD> wrap(
        final int sourceRemote, final long streamID, final Object maybePayload) {
      if (maybePayload instanceof StreamData) {
        return (StreamData<PAYLOAD>) maybePayload;
      } else {
        return new IPCMessage.StreamData<PAYLOAD>(sourceRemote, streamID, (PAYLOAD) maybePayload);
      }
    }

    @Override
    public String toString() {
      return String.format(
          "IPCMessage.StreamData(from:%1$d,stream:%2$d,payload:%3$s)",
          getRemoteID(),
          streamID,
          getPayload());
    }
  }
}
