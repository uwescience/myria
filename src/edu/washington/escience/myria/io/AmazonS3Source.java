/**
 *
 */
package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Objects;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.httpclient.URIException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

/**
 *
 */
@NotThreadSafe
public class AmazonS3Source implements DataSource, Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The logger for debug, trace, etc. messages in this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(AmazonS3Source.class);

  private final URI s3Uri;
  private transient ClientConfiguration clientConfig;
  private transient AmazonS3Client s3Client;
  private transient GetObjectRequest s3Request;

  private long startRange;
  private long endRange;

  private final String bucket;
  private final String key;

  private Long fileSize;

  @JsonCreator
  public AmazonS3Source(
      @JsonProperty(value = "s3Uri", required = true) final String uri,
      @JsonProperty(value = "startRange") final Long startRange,
      @JsonProperty(value = "endRange") final Long endRange)
      throws URIException {
    s3Uri = URI.create(Objects.requireNonNull(uri, "Parameter uri to UriSource may not be null"));
    if (!s3Uri.getScheme().equals("s3")) {
      throw new URIException("URI must contain an S3 scheme");
    }
    String uriString = s3Uri.toString();
    String removedScheme = uriString.substring(5);
    bucket = removedScheme.substring(0, removedScheme.indexOf('/'));
    key = removedScheme.substring(removedScheme.indexOf('/') + 1);

    this.startRange = MoreObjects.firstNonNull(startRange, new Long(0));
    this.endRange = MoreObjects.firstNonNull(endRange, getFileSize());
  }

  public AmazonS3Client getS3Client() {
    if (s3Client == null) {
      clientConfig = new ClientConfiguration();
      clientConfig.setMaxErrorRetry(3);
      s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
    }
    return s3Client;
  }

  public Long getFileSize() {
    if (fileSize == null) {
      fileSize = getS3Client().getObjectMetadata(bucket, key).getContentLength();
    }
    return fileSize;
  }

  public InputStream getInputStream(final long startByte, final long endByte) throws IOException {
    setStartRange(startByte);
    setEndRange(endByte);
    return getInputStream();
  }

  @Override
  public InputStream getInputStream() throws IOException {
    s3Request = new GetObjectRequest(bucket, key);
    s3Request.setRange(startRange, endRange);

    return getS3Client().getObject(s3Request).getObjectContent();
  }

  public void setStartRange(final long startRange) {
    this.startRange = startRange;
  }

  public void setEndRange(final long endRange) {
    this.endRange = endRange;
  }
}
