package edu.washington.escience.myriad.api.encoding;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.api.MyriaApiException;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class DatasetEncoding implements MyriaApiEncoding {
  /** The name of the dataset. */
  public RelationKey relationKey;
  /** The Schema of its tuples. */
  public Schema schema;
  /** The data it contains. */
  public byte[] data;

  @Override
  public void validate() throws MyriaApiException {
    try {
      Preconditions.checkNotNull(relationKey);
      Preconditions.checkNotNull(schema);
      Preconditions.checkNotNull(data);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: relation_key, schema, data");
    }
  }
}