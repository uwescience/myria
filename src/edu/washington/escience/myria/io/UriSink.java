package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.coordinator.CatalogException;

public class UriSink implements DataSink {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @JsonProperty private URI uri;

  public UriSink(@JsonProperty(value = "uri", required = true) final String uri)
      throws CatalogException, URISyntaxException {
    this.uri = URI.create(Objects.requireNonNull(uri, "Parameter uri cannot be null"));
    /* Force using the Hadoop S3A FileSystem */
    if (this.uri.getScheme().equals("s3")) {
      this.uri =
          new URI(
              "s3a",
              this.uri.getUserInfo(),
              this.uri.getHost(),
              this.uri.getPort(),
              this.uri.getPath(),
              this.uri.getQuery(),
              this.uri.getFragment());
    }
    if (!this.uri.getScheme().equals("hdfs") && !this.uri.getScheme().equals("s3a")) {
      throw new CatalogException("URI must be an HDFS or S3 URI");
    }
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    Configuration conf = new Configuration();

    FileSystem fs = FileSystem.get(uri, conf);
    Path rootPath = new Path(uri);
    return fs.create(rootPath);
  }
}
