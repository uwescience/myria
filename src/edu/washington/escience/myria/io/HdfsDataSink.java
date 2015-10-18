package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HdfsDataSink implements DataSink {

  @JsonProperty
  private final URI uri;

  public HdfsDataSink(@JsonProperty(value = "uri", required = true) final String uri) {
    this.uri = URI.create(Objects.requireNonNull(uri, "Parameter uri cannot be null"));
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(uri, conf);
    Path rootPath = new Path(uri);

    return fs.create(rootPath);
  }
}
