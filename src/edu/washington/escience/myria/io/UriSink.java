/**
 *
 */
package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 
 * Similar to UriSource
 */
public class UriSink implements DataSink {

  /** The Uniform Resource Indicator (URI) of the data source. */
  @JsonProperty
  private final String uri;

  /** The logger for debug, trace, etc. messages in this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(UriSink.class);

  public UriSink(@JsonProperty(value = "uri", required = true) final String uri) {
    this.uri = Objects.requireNonNull(uri, "Parameter uri to UriSource may not be null");
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    URI parsedUri = URI.create(uri);

    return (parsedUri.getScheme().equals("http") || parsedUri.getScheme().equals("https")) ? parsedUri.toURL()
        .openConnection().getOutputStream() : getHadoopFileSystemOutputStream(parsedUri);
  }

  private static OutputStream getHadoopFileSystemOutputStream(final URI uri) throws IOException {
    Configuration conf = new Configuration();

    FileSystem fs = FileSystem.get(uri, conf);
    Path rootPath = new Path(uri);

    FSDataOutputStream out = fs.create(rootPath);

    return out;
  }

}
