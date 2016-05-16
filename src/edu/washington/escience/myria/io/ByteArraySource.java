package edu.washington.escience.myria.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A data source that simply wraps a byte array. Note that this does NOT copy the specified array, so the caller MUST
 * NOT mutate it.
 */
public class ByteArraySource implements DataSource, Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The source input stream. */
  @JsonProperty private final byte[] bytes;

  /**
   * Returns a {@link DataSource} that wraps the specified bytes in an {@link InputStream}. Note that this does NOT copy
   * the specified array, so the caller MUST NOT mutate it.
   *
   * @param bytes the data to be produced.
   */
  @JsonCreator
  public ByteArraySource(@JsonProperty(value = "bytes", required = true) final byte[] bytes) {
    this.bytes = Objects.requireNonNull(bytes, "Parameter bytes to ByteSource may not be null");
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return new ByteArrayInputStream(bytes);
  }
}
