package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * A data source that simply wraps an input stream.
 */
public class InputStreamSource implements DataSource {

  /** The source input stream. */
  private final InputStream stream;

  /**
   * Returns a {@link DataSource} that wraps the specified {@link InputStream}.
   *
   * @param stream the source of data.
   */
  public InputStreamSource(final InputStream stream) {
    this.stream =
        Objects.requireNonNull(stream, "Parameter stream to InputStreamSource may not be null");
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return stream;
  }
}
