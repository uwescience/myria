package edu.washington.escience.myriad.api.encoding;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class DatasetEncoding {
  /** The name of the dataset. */
  public RelationKey relationKey;
  /** The Schema of its tuples. */
  public Schema schema;
  /** The data it contains. */
  public byte[] data;
}