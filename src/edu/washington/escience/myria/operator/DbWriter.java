package edu.washington.escience.myria.operator;

import java.util.Map;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.parallel.RelationWriteMetadata;

/** An interface for an operator that writes to a relation in the database. */
public interface DbWriter {
  /**
   * Returns the relations that this operator writes, and their schemas.
   *
   * @return the relations that this operator writes, and their schemas.
   */
  Map<RelationKey, RelationWriteMetadata> writeSet();
}
