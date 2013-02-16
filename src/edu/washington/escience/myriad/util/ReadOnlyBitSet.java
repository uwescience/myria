package edu.washington.escience.myriad.util;

import java.util.BitSet;

/**
 * A read only wrapper class for BitSet. In this way, exposing valid indices in TupleBatch should be safe.
 * */
public final class ReadOnlyBitSet extends BitSet {
  /**
   * 
   * */
  private final BitSet contents;

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public ReadOnlyBitSet(final BitSet contents) {
    super(0);
    this.contents = contents;
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
  public int cardinality() {

    return contents.cardinality();
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

  @Override
  public Object clone() {
    return new ReadOnlyBitSet((BitSet) contents.clone());
  }

  public BitSet cloneAsBitSet() {
    return (BitSet) contents.clone();
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
  public boolean get(final int bitIndex) {
    return contents.get(bitIndex);
  }

  @Override
  public BitSet get(final int fromIndex, final int toIndex) {
    return contents.get(fromIndex, toIndex);
  }

  @Override
  public int hashCode() {
    return contents.hashCode();
  }

  @Override
  public boolean intersects(final BitSet set) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public boolean isEmpty() {
    return contents.isEmpty();
  }

  @Override
  public int length() {
    return contents.length();
  }

  @Override
  public int nextClearBit(final int fromIndex) {
    return contents.nextClearBit(fromIndex);
  }

  @Override
  public int nextSetBit(final int fromIndex) {
    return contents.nextSetBit(fromIndex);
  }

  @Override
  public void or(final BitSet set) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public int previousClearBit(final int fromIndex) {
    return contents.previousClearBit(fromIndex);
  }

  @Override
  public int previousSetBit(final int fromIndex) {
    return contents.previousSetBit(fromIndex);
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
  public int size() {
    return contents.size();
  }

  @Override
  public byte[] toByteArray() {
    return contents.toByteArray();
  }

  @Override
  public long[] toLongArray() {
    return contents.toLongArray();
  }

  @Override
  public String toString() {
    return contents.toString();
  }

  @Override
  public void xor(final BitSet set) {
    throw new UnsupportedOperationException("Read only BitSet");
  }
}
