/**
 *
 */
package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Objects;

import org.apache.commons.httpclient.URIException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 
 */
public class AmazonS3Source implements DataSource, Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The logger for debug, trace, etc. messages in this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(AmazonS3Source.class);

  private final URI s3Uri;
  private transient ClientConfiguration clientConfig;
  private transient AmazonS3Client s3Client;

  private long startRange;
  private long endRange;

  public AmazonS3Source(@JsonProperty(value = "uri", required = true) final String uri) throws URIException {
    s3Uri = URI.create(Objects.requireNonNull(uri, "Parameter uri to UriSource may not be null"));
    /* Force using the Hadoop S3A FileSystem */
    if (!s3Uri.getScheme().equals("s3")) {
      throw new URIException("URI must contain an S3 scheme");
    }
    startRange = 0;
    endRange = getFileSize();
  }

  public AmazonS3Source(@JsonProperty(value = "uri", required = true) final String uri,
      @JsonProperty(value = "startRange", required = true) final long startRange,
      @JsonProperty(value = "endRange", required = true) final long endRange) throws URIException {
    s3Uri = URI.create(Objects.requireNonNull(uri, "Parameter uri to UriSource may not be null"));
    /* Force using the Hadoop S3A FileSystem */
    if (!s3Uri.getScheme().equals("s3")) {
      throw new URIException("URI must contain an S3 scheme");
    }
    this.startRange = startRange;
    this.endRange = endRange;
  }

  public void initializeClient() {
    clientConfig = new ClientConfiguration();
    clientConfig.setMaxErrorRetry(3);
    s3Client = new AmazonS3Client(new AnonymousAWSCredentials());
  }

  @Override
  public InputStream getInputStream() throws IOException {
    String uriString = s3Uri.toString();
    String removedScheme = uriString.substring(5);
    String bucket = removedScheme.substring(0, removedScheme.indexOf('/'));
    String key = removedScheme.substring(removedScheme.indexOf('/') + 1);

    initializeClient();

    GetObjectRequest s3Request = new GetObjectRequest(bucket, key);
    s3Request.setRange(startRange, endRange);
    S3Object s3Object = s3Client.getObject(s3Request);
    return s3Object.getObjectContent();
  }

  public long getFileSize() {
    String uriString = s3Uri.toString();
    String removedScheme = uriString.substring(5);
    String bucket = removedScheme.substring(0, removedScheme.indexOf('/'));
    String key = removedScheme.substring(removedScheme.indexOf('/') + 1);

    initializeClient();

    return s3Client.getObjectMetadata(bucket, key).getContentLength();
  }

  public void setStartRange(final long startRange) {
    this.startRange = startRange;
  }

  public void setEndRange(final long endRange) {
    this.endRange = endRange;
  }
}
