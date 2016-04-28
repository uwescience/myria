package edu.washington.escience.myria.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A data source that pulls data from a specified URI. The URI may be: a path on the local file system; an HDFS link; a
 * web link; an AWS link; and perhaps more.
 *
 * If the URI points to a directory, all files in that directory will be concatenated into a single {@link InputStream}.
 */
public class UriSource implements DataSource, Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The logger for debug, trace, etc. messages in this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(UriSource.class);

  /** The Uniform Resource Indicator (URI) of the data source. */
  private URI parsedUri;

  /**
   * Construct a source of data from the specified URI. The URI may be: a path on the local file system; an HDFS link; a
   * web link; an AWS link; and perhaps more.
   *
   * If the URI points to a directory in HDFS, all files in that directory will be concatenated into a single
   * {@link InputStream}.
   *
   * @param uri the Uniform Resource Indicator (URI) of the data source.
   * @throws URISyntaxException
   */
  @JsonCreator
  public UriSource(@JsonProperty(value = "uri", required = true) final String uri)
      throws URISyntaxException {
    parsedUri =
        URI.create(Objects.requireNonNull(uri, "Parameter uri to UriSource may not be null"));
    /* Force using the Hadoop S3A FileSystem */
    if (parsedUri.getScheme().equals("s3")) {
      parsedUri =
          new URI("s3a", parsedUri.getUserInfo(), parsedUri.getHost(), parsedUri.getPort(), parsedUri.getPath(),
              parsedUri.getQuery(), parsedUri.getFragment());
    }
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return (parsedUri.getScheme().equals("http") || parsedUri.getScheme().equals("https")) ? parsedUri.toURL()
        .openConnection().getInputStream() : getHadoopFileSystemInputStream(parsedUri);
  }

  /**
   * Get an input stream using the configured Hadoop file system for the given URI scheme
   */
  private static InputStream getHadoopFileSystemInputStream(final URI uri) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(uri, conf);
    Path rootPath = new Path(uri);
    FileStatus[] statii = fs.globStatus(rootPath);

    if (statii == null || statii.length == 0) {
      throw new FileNotFoundException(uri.toString());
    }

    List<InputStream> streams = new ArrayList<InputStream>();
    for (FileStatus status : statii) {
      Path path = status.getPath();

      LOGGER.debug("Incorporating input file: " + path);
      streams.add(fs.open(path));
    }

    return new SequenceInputStream(java.util.Collections.enumeration(streams));
  }

}