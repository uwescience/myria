/**
 *
 */
package edu.washington.escience.myria.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class ByteSink implements DataSink {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  transient ByteArrayOutputStream writerOutput;

  @Override
  public OutputStream getOutputStream() throws IOException {
    return new ByteArrayOutputStream();
  }
}