package edu.washington.escience.myria;

import java.io.Serializable;

import edu.washington.escience.myria.util.ImmutableBitSet;

/**
 * The interface of a general purpose filter. This 
 * */
public interface Predicate extends Serializable {

  /**
   * Do the filter. Note that only the valid tuples need to be computed.
   * 
   * @param tb the data to get filtered.
   * @return the filter result, set the bit if a tuple should be kept.
   * */
  ImmutableBitSet filter(final TupleBatch tb);

}
