package edu.washington.escience.myria.operator;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.google.common.collect.ImmutableMap;

import com.google.common.io.LineReader;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.storage.TupleBatch;

import edu.washington.escience.myria.proto.Graph;
import org.apache.commons.io.input.CharSequenceReader;

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
    consumeTuplesProtobufSocket(tuples);
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
