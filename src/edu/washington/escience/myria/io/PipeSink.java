package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.api.PipedStreamingOutput;

public class PipeSink implements DataSink {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private final PipedOutputStream writerOutput;
  private final PipedInputStream input;
  private final PipedStreamingOutput responseEntity;

  @JsonCreator
  public PipeSink() throws IOException {
    writerOutput = new PipedOutputStream();
    input = new PipedInputStream(writerOutput, MyriaConstants.DEFAULT_PIPED_INPUT_STREAM_SIZE);
    responseEntity = new PipedStreamingOutput(input);
  }

  @Override
  public OutputStream getOutputStream() {
    return writerOutput;
  }

  public PipedStreamingOutput getResponse() {
    return responseEntity;
  }

  public InputStream getInputStream() {
    return input;
  }
}
