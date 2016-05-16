package edu.washington.escience.myria.util;

import java.util.BitSet;

/**
 * A read only wrapper class for BitSet. In this way, exposing valid indices in TupleBatch should be safe.
 * */
public final class ImmutableBitSet extends BitSet {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Wrap a BitSet so the the BitSet is readonly.
   *
   * @param contents the BitSet to be wrapped
   * */
  public ImmutableBitSet(final BitSet contents) {
    super(contents.cardinality());
    super.or(contents);
  }

  @Override
  public void and(final BitSet set) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void andNot(final BitSet set) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void clear(final int bitIndex) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void clear(final int fromIndex, final int toIndex) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  /**
   * @return An ImmutableBitSet.
   * */
  @Override
  public ImmutableBitSet clone() {
    return (ImmutableBitSet) super.clone();
  }

  /**
   * Clone as an ordinary BitSet.
   *
   * @return the cloned BitSet.
   * */
  public BitSet cloneAsBitSet() {
    return BitSet.valueOf(toByteArray());
  }

  @Override
  public void flip(final int bitIndex) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void flip(final int fromIndex, final int toIndex) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public boolean intersects(final BitSet set) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void or(final BitSet set) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void set(final int bitIndex) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void set(final int bitIndex, final boolean value) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void set(final int fromIndex, final int toIndex) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void set(final int fromIndex, final int toIndex, final boolean value) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void xor(final BitSet set) {
    throw new UnsupportedOperationException("Read only BitSet");
  }
}
