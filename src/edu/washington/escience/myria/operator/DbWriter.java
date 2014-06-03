package edu.washington.escience.myria.operator;

import java.util.Set;

import edu.washington.escience.myria.RelationKey;

/** An interface for an operator that writes to a relation in the database. */
public interface DbWriter {
  /**
   * Returns the set of relations that this operator writes.
   * 
   * @return the set of relations that this operator writes.
   */
  Set<RelationKey> writeSet();
}
