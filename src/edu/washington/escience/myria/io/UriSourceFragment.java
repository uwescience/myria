/**
 *
 */
package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * 
 */
public class UriSourceFragment implements DataSource, Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The logger for debug, trace, etc. messages in this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(UriSource.class);

  /** The Uniform Resource Indicator (URI) of the data source. */
  final String bucket;
  final String key;
  final long startRange;
  final long endRange;

  /**
   * Might be merged with UriSource or separate class
   */
  public UriSourceFragment(final String bucket, final String key, final long startRange, final long endRange,
      final boolean lastWorker) {
    this.bucket = bucket;
    this.key = key;
    this.startRange = startRange;
    this.endRange = endRange;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    // A chunked input stream from start and end range using S3 api
    AmazonS3 s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
    GetObjectRequest s3Request = new GetObjectRequest(bucket, key);
    s3Request.setRange(startRange, endRange);
    S3Object s3Object = s3Client.getObject(s3Request);
    return s3Object.getObjectContent();
  }
}
