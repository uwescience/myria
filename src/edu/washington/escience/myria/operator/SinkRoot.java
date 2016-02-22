package edu.washington.escience.myria.operator;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import com.google.common.collect.ImmutableMap;

import com.google.common.io.LineReader;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.storage.TupleBatch;

import edu.washington.escience.myria.operator.mmap.MemoryMappedFileBuffer;
import edu.washington.escience.myria.proto.Graph;
//import io.netty.buffer.ByteBuf;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.types.Types;
import org.apache.commons.io.input.CharSequenceReader;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.codec.compression.ZlibWrapper;

/**
 * A root operator that simply count the number of results and drop them.
 *
 * This RootOperator is a reasonable RootOperator for master plans which are not aiming at importing data into workers.
 * */
public class SinkRoot extends RootOperator {
  private transient OutputStream outputStream;

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Number of tuples.
   * */
  private long cnt;

  /**
   * The maximum number of tuples to read. If limit <= 0, all tuples will be read. Note that this is only accurate to
   * the nearest TupleBatch over limit.
   */
  private long limit;

  /**
   * @return count.
   * */
  public final long getCount() {
    return cnt;
  }

  /**
   * @param child the child.
   * */
  public SinkRoot(final Operator child) {
    this(child, 0);
  }

  /**
   * @param child the child.
   * @param limit the limit of the number of tuples this operator will absorb. If limit <= 0, all tuples will be read.
   *          Note that this is only accurate to the nearest TupleBatch over limit.
   * */
  public SinkRoot(final Operator child, final long limit) {
    super(child);
    setLimit(limit);
  }

  @Override
  protected final void consumeTuples(final TupleBatch tuples) throws DbException {
    consumeTuplesArrow(tuples);
    //consumeTuplesArrowSharedMemory(tuples);
  }

  private final void consumeTuplesBinary(final TupleBatch tuples) throws DbException {
    cnt += tuples.numTuples();

    ByteBuffer buffer = ByteBuffer.allocate(1*8 + 4*8 + 3*4);

    /* Serialize every row into the output stream. */
    try {
      for (int row = 0; row < tuples.numTuples(); ++row) {
        buffer.clear();
        buffer.putLong(tuples.getLong(0, row));
        buffer.putDouble(tuples.getDouble(1, row));
        buffer.putInt((int) tuples.getLong(2, row));
        buffer.putDouble(tuples.getDouble(3, row));
        buffer.putInt((int) tuples.getLong(4, row));
        buffer.putDouble(tuples.getDouble(5, row));
        buffer.putInt((int) tuples.getLong(6, row));
        buffer.putDouble(tuples.getDouble(7, row));
        outputStream.write(buffer.array());
      }
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  ServerSocket serverSocket = null;
  Socket socket = null;

  private final void consumeTuplesBinarySocket(final TupleBatch tuples) throws DbException {
    cnt += tuples.numTuples();

    ByteBuffer buffer = ByteBuffer.allocate(1*8 + 4*8 + 3*4);

    /* Serialize every row into the output stream. */
    try {
      int port = Integer.parseInt(new LineReader(new InputStreamReader(new FileInputStream("/tmp/port"))).readLine());
      if(serverSocket == null)
        serverSocket = new ServerSocket(port);
      if(socket == null)
        socket = serverSocket.accept();
      OutputStream stream  = socket.getOutputStream();

      for (int row = 0; row < tuples.numTuples(); ++row) {
        buffer.clear();
        buffer.putLong(tuples.getLong(0, row));
        buffer.putDouble(tuples.getDouble(1, row));
        buffer.putInt((int) tuples.getLong(2, row));
        buffer.putDouble(tuples.getDouble(3, row));
        buffer.putInt((int) tuples.getLong(4, row));
        buffer.putDouble(tuples.getDouble(5, row));
        buffer.putInt((int) tuples.getLong(6, row));
        buffer.putDouble(tuples.getDouble(7, row));
        stream.write(buffer.array());
      }
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  private final void consumeTuplesProtobuf(final TupleBatch tuples) throws DbException {
    cnt += tuples.numTuples();

    Graph.Vertices.Builder builder = Graph.Vertices.newBuilder();

    /* Serialize every row into the output stream. */
    for (int row = 0; row < tuples.numTuples(); ++row) {
      builder.addVertices(Graph.Vertex.newBuilder()
              .setId((int)tuples.getLong(0, row))
              .setValue(tuples.getDouble(1, row))
              .addEdge(Graph.Vertex.Edge.newBuilder()
                                   .setDestinationId((int)tuples.getLong(2, row))
                                   .setValue(tuples.getDouble(3, row)))
              .addEdge(Graph.Vertex.Edge.newBuilder()
                                   .setDestinationId((int)tuples.getLong(4, row))
                                   .setValue(tuples.getDouble(5, row)))
              .addEdge(Graph.Vertex.Edge.newBuilder()
                                   .setDestinationId((int)tuples.getLong(6, row))
                                   .setValue(tuples.getDouble(7, row))));
    }

    try {
      Graph.Vertices vertices = builder.build();
      outputStream.write(ByteBuffer.allocate(4).putInt(vertices.getSerializedSize()).array());
      outputStream.write(vertices.toByteArray());
    } catch(IOException e) {
      throw new DbException(e);
    }

/*
    if (limit > 0 && cnt >= limit) {
      setEOS();
    }
*/
  }

  private final static int ALLOCATION_SIZE = BaseValueVector.INITIAL_VALUE_ALLOCATION * 8 * 1024;
  private final static int BUFFER_SIZE = ALLOCATION_SIZE / 2;
  private transient BufferAllocator allocator = null;
  private int port;
  private transient ByteBuffer sizeBuffer = null;
  private MemoryMappedFileBuffer sharedMemory0, sharedMemory1, sharedMemory2, sharedMemory3, sharedMemory4, sharedMemory5, sharedMemory6, sharedMemory7;
  UInt8Vector vector0;
  UInt8Vector vector2;
  UInt8Vector vector4;
  UInt8Vector vector6;
  Float8Vector vector1;
  Float8Vector vector3;
  Float8Vector vector5;
  Float8Vector vector7;

  private final void consumeTuplesArrowSharedMemory(final TupleBatch tuples) throws DbException {
    if(allocator == null) {
      allocator = new RootAllocator(ALLOCATION_SIZE);
      sizeBuffer = ByteBuffer.allocate(4);
      try {
        port = Integer.parseInt(new LineReader(new InputStreamReader(new FileInputStream("/tmp/port"))).readLine());
        sharedMemory0 = new MemoryMappedFileBuffer(new File("/tmp/vector0"), TupleBatch.BATCH_SIZE * 8);
        sharedMemory1 = new MemoryMappedFileBuffer(new File("/tmp/vector1"), TupleBatch.BATCH_SIZE * 8);
        sharedMemory2 = new MemoryMappedFileBuffer(new File("/tmp/vector2"), TupleBatch.BATCH_SIZE * 8);
        sharedMemory3 = new MemoryMappedFileBuffer(new File("/tmp/vector3"), TupleBatch.BATCH_SIZE * 8);
        sharedMemory4 = new MemoryMappedFileBuffer(new File("/tmp/vector4"), TupleBatch.BATCH_SIZE * 8);
        sharedMemory5 = new MemoryMappedFileBuffer(new File("/tmp/vector5"), TupleBatch.BATCH_SIZE * 8);
        sharedMemory6 = new MemoryMappedFileBuffer(new File("/tmp/vector6"), TupleBatch.BATCH_SIZE * 8);
        sharedMemory7 = new MemoryMappedFileBuffer(new File("/tmp/vector7"), TupleBatch.BATCH_SIZE * 8);
        vector0 = new UInt8Vector(MaterializedField.create("field0", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
        vector2 = new UInt8Vector(MaterializedField.create("field2", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
        vector4 = new UInt8Vector(MaterializedField.create("field4", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
        vector6 = new UInt8Vector(MaterializedField.create("field6", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
        vector1 = new Float8Vector(MaterializedField.create("field1", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
        vector3 = new Float8Vector(MaterializedField.create("field3", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
        vector5 = new Float8Vector(MaterializedField.create("field5", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
        vector7 = new Float8Vector(MaterializedField.create("field7", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
      } catch(IOException e) {
        throw new RuntimeException(e);
      }
    }

    vector0.allocateNew(tuples.numTuples());
    vector1.allocateNew(tuples.numTuples());
    vector2.allocateNew(tuples.numTuples());
    vector3.allocateNew(tuples.numTuples());
    vector4.allocateNew(tuples.numTuples());
    vector5.allocateNew(tuples.numTuples());
    vector6.allocateNew(tuples.numTuples());
    vector7.allocateNew(tuples.numTuples());

    UInt8Vector.Mutator mutator0 = vector0.getMutator();
    Float8Vector.Mutator mutator1 = vector1.getMutator();
    UInt8Vector.Mutator mutator2 = vector2.getMutator();
    Float8Vector.Mutator mutator3 = vector3.getMutator();
    UInt8Vector.Mutator mutator4 = vector4.getMutator();
    Float8Vector.Mutator mutator5 = vector5.getMutator();
    UInt8Vector.Mutator mutator6 = vector6.getMutator();
    Float8Vector.Mutator mutator7 = vector7.getMutator();

    for (int row = 0; row < tuples.numTuples(); ++row) {
      mutator0.set(row, tuples.getLong(0, row));
      mutator1.set(row, tuples.getDouble(1, row));
      mutator2.set(row, tuples.getLong(2, row));
      mutator3.set(row, tuples.getDouble(3, row));
      mutator4.set(row, tuples.getLong(4, row));
      mutator5.set(row, tuples.getDouble(5, row));
      mutator6.set(row, tuples.getLong(6, row));
      mutator7.set(row, tuples.getDouble(7, row));
    }

    mutator0.setValueCount(tuples.numTuples());
    mutator1.setValueCount(tuples.numTuples());
    mutator2.setValueCount(tuples.numTuples());
    mutator3.setValueCount(tuples.numTuples());
    mutator4.setValueCount(tuples.numTuples());
    mutator5.setValueCount(tuples.numTuples());
    mutator6.setValueCount(tuples.numTuples());
    mutator7.setValueCount(tuples.numTuples());

    try {
      if(serverSocket == null)
        serverSocket = new ServerSocket(port);
      if(socket == null)
        socket = serverSocket.accept();
      OutputStream stream  = socket.getOutputStream();

      sharedMemory0.buffer().writerIndex(0);
      sharedMemory1.buffer().writerIndex(0);
      sharedMemory2.buffer().writerIndex(0);
      sharedMemory3.buffer().writerIndex(0);
      sharedMemory4.buffer().writerIndex(0);
      sharedMemory5.buffer().writerIndex(0);
      sharedMemory6.buffer().writerIndex(0);
      sharedMemory7.buffer().writerIndex(0);

      sharedMemory0.buffer().writeBytes(vector0.getBuffer());
      sharedMemory1.buffer().writeBytes(vector1.getBuffer());
      sharedMemory2.buffer().writeBytes(vector2.getBuffer());
      sharedMemory3.buffer().writeBytes(vector3.getBuffer());
      sharedMemory4.buffer().writeBytes(vector4.getBuffer());
      sharedMemory5.buffer().writeBytes(vector5.getBuffer());
      sharedMemory6.buffer().writeBytes(vector6.getBuffer());
      sharedMemory7.buffer().writeBytes(vector7.getBuffer());

      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(1).array());
    } catch(IOException e) {
      throw new DbException(e);
    }

    vector0.close();
    vector1.close();
    vector2.close();
    vector3.close();
    vector4.close();
    vector5.close();
    vector6.close();
    vector7.close();
  }

  private final void consumeTuplesArrow(final TupleBatch tuples) throws DbException {
    if(allocator == null) {
      allocator = new RootAllocator(ALLOCATION_SIZE);
      sizeBuffer = ByteBuffer.allocate(4);
      try {
        port = Integer.parseInt(new LineReader(new InputStreamReader(new FileInputStream("/tmp/port"))).readLine());
      } catch(IOException e) {
        throw new RuntimeException(e);
      }
    }

    //ArrowBuf buffer = rootAllocator.buffer(BUFFER_SIZE);
    UInt8Vector vector0 = new UInt8Vector(MaterializedField.create("field0", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
    UInt8Vector vector2 = new UInt8Vector(MaterializedField.create("field2", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
    UInt8Vector vector4 = new UInt8Vector(MaterializedField.create("field4", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
    UInt8Vector vector6 = new UInt8Vector(MaterializedField.create("field6", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
    Float8Vector vector1 = new Float8Vector(MaterializedField.create("field1", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
    Float8Vector vector3 = new Float8Vector(MaterializedField.create("field3", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
    Float8Vector vector5 = new Float8Vector(MaterializedField.create("field5", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
    Float8Vector vector7 = new Float8Vector(MaterializedField.create("field7", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
    vector0.allocateNew(tuples.numTuples());
    vector1.allocateNew(tuples.numTuples());
    vector2.allocateNew(tuples.numTuples());
    vector3.allocateNew(tuples.numTuples());
    vector4.allocateNew(tuples.numTuples());
    vector5.allocateNew(tuples.numTuples());
    vector6.allocateNew(tuples.numTuples());
    vector7.allocateNew(tuples.numTuples());

    UInt8Vector.Mutator mutator0 = vector0.getMutator();
    Float8Vector.Mutator mutator1 = vector1.getMutator();
    UInt8Vector.Mutator mutator2 = vector2.getMutator();
    Float8Vector.Mutator mutator3 = vector3.getMutator();
    UInt8Vector.Mutator mutator4 = vector4.getMutator();
    Float8Vector.Mutator mutator5 = vector5.getMutator();
    UInt8Vector.Mutator mutator6 = vector6.getMutator();
    Float8Vector.Mutator mutator7 = vector7.getMutator();

    for (int row = 0; row < tuples.numTuples(); ++row) {
      mutator0.set(row, tuples.getLong(0, row));
      mutator1.set(row, tuples.getDouble(1, row));
      mutator2.set(row, tuples.getLong(2, row));
      mutator3.set(row, tuples.getDouble(3, row));
      mutator4.set(row, tuples.getLong(4, row));
      mutator5.set(row, tuples.getDouble(5, row));
      mutator6.set(row, tuples.getLong(6, row));
      mutator7.set(row, tuples.getDouble(7, row));
    }

    mutator0.setValueCount(tuples.numTuples());
    mutator1.setValueCount(tuples.numTuples());
    mutator2.setValueCount(tuples.numTuples());
    mutator3.setValueCount(tuples.numTuples());
    mutator4.setValueCount(tuples.numTuples());
    mutator5.setValueCount(tuples.numTuples());
    mutator6.setValueCount(tuples.numTuples());
    mutator7.setValueCount(tuples.numTuples());
    //IntVector.Accessor accessor = vector.getAccessor();

    /*
    try {
      ByteBuf buf = new MemoryMappedFileBuffer(new File("/tmp/buffer0"), vector0.getBufferSize()).buffer();
      buf.writeBytes(vector0.getBuffer());
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
    */

    try {
      ByteBuffer compressed;

      if(serverSocket == null)
        serverSocket = new ServerSocket(port);
      if(socket == null)
        socket = serverSocket.accept();
      OutputStream stream  = socket.getOutputStream();

      compressed = compressBuffer(vector0.getBuffer().nioBuffer());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(compressed.remaining()).array());
      stream.write(compressed.array(), 0, compressed.remaining());
      compressed = compressBuffer(vector1.getBuffer().nioBuffer());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(compressed.remaining()).array());
      stream.write(compressed.array(), 0, compressed.remaining());
      compressed = compressBuffer(vector2.getBuffer().nioBuffer());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(compressed.remaining()).array());
      stream.write(compressed.array(), 0, compressed.remaining());
      compressed = compressBuffer(vector3.getBuffer().nioBuffer());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(compressed.remaining()).array());
      stream.write(compressed.array(), 0, compressed.remaining());
      compressed = compressBuffer(vector4.getBuffer().nioBuffer());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(compressed.remaining()).array());
      stream.write(compressed.array(), 0, compressed.remaining());
      compressed = compressBuffer(vector5.getBuffer().nioBuffer());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(compressed.remaining()).array());
      stream.write(compressed.array(), 0, compressed.remaining());
      compressed = compressBuffer(vector6.getBuffer().nioBuffer());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(compressed.remaining()).array());
      stream.write(compressed.array(), 0, compressed.remaining());
      compressed = compressBuffer(vector7.getBuffer().nioBuffer());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(compressed.remaining()).array());
      stream.write(compressed.array(), 0, compressed.remaining());

      /*
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(vector0.getBufferSize()).array());
      vector0.getBuffer().getBytes(0, stream, vector0.getBufferSize());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(vector1.getBufferSize()).array());
      vector1.getBuffer().getBytes(0, stream, vector1.getBufferSize());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(vector2.getBufferSize()).array());
      vector2.getBuffer().getBytes(0, stream, vector2.getBufferSize());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(vector3.getBufferSize()).array());
      vector3.getBuffer().getBytes(0, stream, vector3.getBufferSize());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(vector4.getBufferSize()).array());
      vector4.getBuffer().getBytes(0, stream, vector4.getBufferSize());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(vector5.getBufferSize()).array());
      vector5.getBuffer().getBytes(0, stream, vector5.getBufferSize());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(vector6.getBufferSize()).array());
      vector6.getBuffer().getBytes(0, stream, vector6.getBufferSize());
      stream.write(((ByteBuffer)sizeBuffer.clear()).putInt(vector7.getBufferSize()).array());
      vector7.getBuffer().getBytes(0, stream, vector7.getBufferSize());
      */
      //sizeBuffer.clear();
      //stream.write(sizeBuffer.putInt(tuples.numTuples()).array());
      //stream.write(vector0.getBuffer().array());
      //stream.write(vector1.getBuffer().array());
      //stream.write(vector2.getBuffer().array());
      //stream.write(vector3.getBuffer().array());
      //stream.write(vector4.getBuffer().array());
      //stream.write(vector5.getBuffer().array());
      //stream.write(vector6.getBuffer().array());
      //stream.write(vector7.getBuffer().array());
    } catch(IOException e) {
      throw new DbException(e);
    }

    vector0.close();
    vector1.close();
    vector2.close();
    vector3.close();
    vector4.close();
    vector5.close();
    vector6.close();
    vector7.close();
  }

  private ByteBuffer compressBuffer(ByteBuffer input) {
    Deflater deflater = new Deflater(Deflater.BEST_SPEED);
    byte[] data = new byte[input.remaining()];
    input.get(data);
    deflater.setInput(data);
    deflater.finish();

    ByteBuffer buffer = ByteBuffer.allocate(data.length + 100);
    int size = deflater.deflate(buffer.array());
    deflater.end();

    buffer.limit(size);
    return buffer;
  }

  private ByteBuffer compressBuffer(byte[] data) {
    Deflater deflater = new Deflater(Deflater.BEST_SPEED);
    deflater.setInput(data);
    deflater.finish();

    ByteBuffer buffer = ByteBuffer.allocate(data.length + 100);
    int size = deflater.deflate(buffer.array());
    deflater.end();

    buffer.limit(size);
    return buffer;
  }

  private final void consumeTuplesProtobufSocket(final TupleBatch tuples) throws DbException {
    cnt += tuples.numTuples();

    Graph.Vertices.Builder builder = Graph.Vertices.newBuilder();

    /* Serialize every row into the output stream. */
    for (int row = 0; row < tuples.numTuples(); ++row) {
      builder.addVertices(Graph.Vertex.newBuilder()
              .setId((int)tuples.getLong(0, row))
              .setValue(tuples.getDouble(1, row))
              .addEdge(Graph.Vertex.Edge.newBuilder()
                      .setDestinationId((int)tuples.getLong(2, row))
                      .setValue(tuples.getDouble(3, row)))
              .addEdge(Graph.Vertex.Edge.newBuilder()
                      .setDestinationId((int)tuples.getLong(4, row))
                      .setValue(tuples.getDouble(5, row)))
              .addEdge(Graph.Vertex.Edge.newBuilder()
                      .setDestinationId((int)tuples.getLong(6, row))
                      .setValue(tuples.getDouble(7, row))));
    }

    try {
      Graph.Vertices vertices = builder.build();

      int port = Integer.parseInt(new LineReader(new InputStreamReader(new FileInputStream("/tmp/port"))).readLine());
      if(serverSocket == null)
        serverSocket = new ServerSocket(port);
      if(socket == null)
        socket = serverSocket.accept();
      OutputStream stream  = socket.getOutputStream();

      stream.write(ByteBuffer.allocate(4).putInt(vertices.getSerializedSize()).array());
      stream.write(vertices.toByteArray());
    } catch(IOException e) {
      throw new DbException(e);
    }

/*
    if (limit > 0 && cnt >= limit) {
      setEOS();
    }
*/
  }

  @Override
  protected void childEOS() throws DbException {
  }

  @Override
  protected void childEOI() throws DbException {
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    cnt = 0;
    try {
      outputStream = new BufferedOutputStream(new FileOutputStream("/tmp/protobuf"));
    } catch(IOException e) {
      throw new DbException(e);
    }

    //throw new DbException("opened");
  }

  @Override
  protected void cleanup() throws DbException {
    try {
      outputStream.close();
      if(serverSocket != null)
        serverSocket.close();
      if(socket != null)
        socket.close();
    } catch(IOException e) {
      throw new DbException(e);
    }
  }

  /**
   * Set the limit of the number of tuples this operator will absorb. If limit <= 0, all tuples will be read. Note that
   * this is only accurate to the nearest TupleBatch over limit.
   *
   * @param limit the number of tuples this operator will absorb. If limit <= 0, all tuples will be read. Note that this
   *          is only accurate to the nearest TupleBatch over limit.
   */
  public final void setLimit(final long limit) {
    this.limit = limit;
  }
}
