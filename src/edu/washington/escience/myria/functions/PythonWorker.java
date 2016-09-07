/**
 *
 */
package edu.washington.escience.myria.functions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.ProcessBuilder.Redirect;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Type;

/**
 *
 */
public class PythonWorker {
  /***/
  private static final long serialVersionUID = 1L;
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PythonWorker.class);

  private ServerSocket serverSocket = null;
  private Socket clientSock = null;
  private Process worker = null;
  private DataOutputStream dOut;
  private DataInputStream dIn;

  /**
   *
   * @param child child operator that data is fetched from
   * @param emitExpressions expression that created the output
   * @throws DbException
   */
  public PythonWorker() throws DbException {

    try {
      createServerSocket();
      startPythonWorker();

    } catch (Exception e) {
      LOGGER.info(e.getMessage());
      throw new DbException("Failed to create Python Worker");
    }
  }

  /**
   *
   * @param pyCodeString - python function string
   * @param tupleSize
   * @param outputType
   * @throws DbException
   */
  public void sendCodePickle(final String pyCodeString, final int tupleSize, final Type outputType, final int isFlatMap)
      throws DbException {
    Preconditions.checkNotNull(pyCodeString);

    try {
      if (pyCodeString.length() > 0 && dOut != null) {
        // LOGGER.info("length of the code String: " + pyCodeString.length());
        byte[] bytes = pyCodeString.getBytes(StandardCharsets.UTF_8);
        dOut.writeInt(bytes.length);
        dOut.write(bytes);

        dOut.writeInt(tupleSize);
        writeOutputType(outputType);
        dOut.writeInt(isFlatMap);

        dOut.flush();
        // LOGGER.info("wrote and flushed code snippet ");
      } else {
        LOGGER.info("something is very wrong, python code  or output stream are empty");
        throw new DbException("Can't write pythonCode to worker!");
      }
    } catch (Exception e) {
      LOGGER.info("failed to send python code pickle");
      throw new DbException(e);
    }
  }

  public void sendNumTuples(final int numTuples) throws IOException, DbException {
    Preconditions.checkArgument(numTuples > 0, "number of tuples: %s", numTuples);
    try {
      dOut.writeInt(numTuples);
    } catch (Exception e) {
      LOGGER.info("failed to write number of tuples to python process!");
      throw new DbException(e);
    }
  }

  /**
   *
   * @return dataoutput stream for the python worker.
   */
  public DataOutputStream getDataOutputStream() {
    Preconditions.checkNotNull(dOut);
    return dOut;
  }

  /**
   *
   * @return dataInputStream for the python worker.
   */
  public DataInputStream getDataInputStream() {
    Preconditions.checkNotNull(dIn);
    return dIn;
  }

  /**
   *
   * @throws IOException
   */
  public void close() throws IOException {
    if (clientSock != null) {
      clientSock.close();
    }

    if (serverSocket != null) {
      serverSocket.close();
    }
    // stop worker process
    if (worker != null) {
      worker.destroy();
    }
  }

  /**
   *
   * @throws UnknownHostException
   * @throws IOException
   */
  private void createServerSocket() throws UnknownHostException, IOException {

    serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
    int a = serverSocket.getLocalPort();
    LOGGER.info("created socket " + a);
  }

  /**
   *
   * @throws IOException
   */
  private void startPythonWorker() throws IOException {

    String pythonWorker = MyriaConstants.PYTHONWORKER;
    ProcessBuilder pb = new ProcessBuilder(MyriaConstants.PYTHONEXEC, "-m", pythonWorker);
    final Map<String, String> env = pb.environment();
    StringBuilder sb = new StringBuilder();
    sb.append(System.getenv("HOME"));
    sb.append("/anaconda2/bin");
    sb.append(":");
    sb.append(env.get("PATH"));
    env.put("PATH", sb.toString());

    env.put("PYTHONUNBUFFERED", "YES");

    pb.redirectError(Redirect.INHERIT);
    pb.redirectOutput(Redirect.INHERIT);

    // write the env variables to the path of the starting process
    worker = pb.start();
    // LOGGER.info("Started the python process");
    OutputStream stdin = worker.getOutputStream();
    OutputStreamWriter out = new OutputStreamWriter(stdin, StandardCharsets.UTF_8);

    out.write(serverSocket.getLocalPort() + "\n");
    out.flush();
    clientSock = serverSocket.accept();
    LOGGER.info("successfully launched worker");
    setupStreams();

    return;
  }

  /**
   *
   * @param outputType
   * @throws IOException
   * @throws DbException
   */
  private void writeOutputType(final Type outputType) throws IOException, DbException {
    switch (outputType) {
      case DOUBLE_TYPE:
        dOut.writeInt(MyriaConstants.PythonType.DOUBLE.getVal());
        break;
      case FLOAT_TYPE:
        dOut.writeInt(MyriaConstants.PythonType.FLOAT.getVal());
        break;
      case INT_TYPE:
        dOut.writeInt(MyriaConstants.PythonType.INT.getVal());
        break;
      case LONG_TYPE:
        dOut.writeInt(MyriaConstants.PythonType.LONG.getVal());
        break;
      case BYTES_TYPE:
        dOut.writeInt(MyriaConstants.PythonType.BYTES.getVal());
        break;
      default:
        throw new DbException("Type not supported for python UDF ");
    }
  }

  /**
   *
   * @throws IOException
   */
  private void setupStreams() throws IOException {
    if (clientSock != null) {
      dOut = new DataOutputStream(clientSock.getOutputStream());
      dIn = new DataInputStream(clientSock.getInputStream());
    }
  }
}
