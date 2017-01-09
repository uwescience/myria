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
  /** logger*/
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(PythonWorker.class);

  /** server socket for python worker.*/
  private ServerSocket serverSocket = null;
  /** client sock for python worker.*/
  private Socket clientSock = null;
  /** python worker process.*/
  private Process worker = null;
  /** output stream from python worker.*/
  private DataOutputStream dOut;
  /**input stream from python worker.*/
  private DataInputStream dIn;

  /**
   *
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
   * @param numColumns number fo columns to be written to python process.
   * @param outputType output type of the python function.
   * @param isFlatMap does the python function return multiple tuples for a single input?
   * @throws DbException in case of error.
   */
  public void sendCodePickle(
      final String pyCodeString,
      final int numColumns,
      final Type outputType,
      final Boolean isFlatMap)
      throws DbException {
    Preconditions.checkNotNull(pyCodeString);

    try {
      if (pyCodeString.length() > 0 && dOut != null) {
        byte[] bytes = pyCodeString.getBytes(StandardCharsets.UTF_8);
        dOut.writeInt(bytes.length);
        dOut.write(bytes);

        dOut.writeInt(numColumns);
        writeOutputType(outputType);
        if (isFlatMap) {
          dOut.writeInt(1);
        } else {
          dOut.writeInt(0);
        }
        dOut.flush();

      } else {
        throw new DbException("Can't write Python Code to worker!");
      }
    } catch (Exception e) {
      LOGGER.info("failed to send python code pickle");
      throw new DbException(e);
    }
  }
  /**
   *
   * @param numTuples: number of tuples to be sent to python function.
   * @throws IOException
   * @throws DbException
   */
  public void sendNumTuples(final int numTuples) throws IOException, DbException {
    Preconditions.checkArgument(numTuples > 0, "number of tuples: %s", numTuples);
    try {
      dOut.writeInt(numTuples);
    } catch (Exception e) {
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
  }

  /**
   *
   * @throws IOException
   */
  private void startPythonWorker() throws IOException {

    String pythonWorker = MyriaConstants.PYTHONWORKER;
    ProcessBuilder pb = new ProcessBuilder(MyriaConstants.PYTHONEXEC, "-m", pythonWorker);
    final Map<String, String> env = pb.environment();

    env.put("PYTHONUNBUFFERED", "YES");

    pb.redirectError(Redirect.INHERIT);
    pb.redirectOutput(Redirect.INHERIT);

    // write the env variables to the path of the starting process
    worker = pb.start();
    OutputStream stdin = worker.getOutputStream();
    OutputStreamWriter out = new OutputStreamWriter(stdin, StandardCharsets.UTF_8);

    out.write(serverSocket.getLocalPort() + "\n");
    out.flush();
    clientSock = serverSocket.accept();
    setupStreams();
    return;
  }

  /**
   *
   * @param outputType : output type for python function
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
      case BLOB_TYPE:
        dOut.writeInt(MyriaConstants.PythonType.BLOB.getVal());
        break;
      default:
        throw new DbException("Type not supported for python UDF ");
    }
  }

  /**
   *
   * @throws IOException in case of error.
   */
  private void setupStreams() throws IOException {
    if (clientSock != null) {
      dOut = new DataOutputStream(clientSock.getOutputStream());
      dIn = new DataInputStream(clientSock.getInputStream());
    }
  }
  /**
   *
   * @param EOS Send end of stream to cleanly close the python process.
   * @throws DbException in case of error.
   */
  public void sendEos(final int EOS) throws DbException {
    try {
      dOut.writeInt(EOS);
      close();
    } catch (Exception e) {
      throw new DbException(e);
    }
  }
}
