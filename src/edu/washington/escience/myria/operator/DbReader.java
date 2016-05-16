package edu.washington.escience.myria.operator;

import java.util.Set;

import edu.washington.escience.myria.RelationKey;

/** An interface for an operator that reads from a relation in the database. */
public interface DbReader {
  /**
   * Returns the set of relations that this operator reads.
   *
   * @return the set of relations that this operator reads.
   */
  Set<RelationKey> readSet();
}
