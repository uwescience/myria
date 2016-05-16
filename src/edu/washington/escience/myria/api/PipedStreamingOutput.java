package edu.washington.escience.myria.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.io.IOUtils;

import com.google.common.io.ByteStreams;

/**
 * A {@link PipedStreamingOutput} is a {@link StreamingOutput} that simply copies a supplied {@link InputStream} to the
 * streaming {@link OutputStream}. This class is used to serialize objects to an HTTP client.
 *
 *
 */
public final class PipedStreamingOutput implements StreamingOutput {
  /** The input stream is the source of the bytes to be streamed to the client. */
  private final InputStream input;

  /**
   * Construct a new {@StreamingOutput} that will simply stream the bytes from the supplied input to
   * the client.
   *
   * @param input the source of the bytes to be streamed to the client.
   */
  public PipedStreamingOutput(final InputStream input) {
    this.input = Objects.requireNonNull(input);
  }

  @Override
  public void write(final OutputStream output) throws IOException, WebApplicationException {
    try {
      ByteStreams.copy(input, output);
    } finally {
      IOUtils.closeQuietly(input);
      IOUtils.closeQuietly(output);
    }
  }
}
