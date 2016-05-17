package edu.washington.escience.myria.io;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A data source that pulls data from local file.
 */
public class FileSource implements DataSource, Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The filename. */
  @JsonProperty private final String filename;

  /**
   * Construct a source of data that pulls from a local file.
   *
   * @param filename the local file to be read.
   */
  @JsonCreator
  public FileSource(@JsonProperty(value = "filename", required = true) final String filename) {
    this.filename =
        Objects.requireNonNull(filename, "Parameter filename to FileSource may not be null");
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return new FileInputStream(filename);
  }

  /**
   * @return the local file that this FileSource references.
   */
  public String getFilename() {
    return filename;
  }
}
