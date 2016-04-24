package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.coordinator.CatalogException;

public class FileSink implements DataSink {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @JsonProperty
  private String filename;

  public FileSink(@JsonProperty(value = "filename", required = true) final String filename) throws CatalogException {
    this.filename = filename;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return new FileOutputStream(filename);
  }
}