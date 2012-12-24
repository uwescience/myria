package edu.washington.escience.myriad.util;

import java.util.BitSet;

/**
 * A read only wrapper class for BitSet. In this way, exposing valid indices in TupleBatch should be safe.
 * */
public class ReadOnlyBitSet extends BitSet {
  /**
   * 
   * */
  private final BitSet contents;

  public ReadOnlyBitSet(BitSet contents) {
    super(0);
    this.contents = contents;
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public void and(BitSet set) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void andNot(BitSet set) {
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
  public void clear(int bitIndex) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void clear(int fromIndex, int toIndex) {
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
  public void flip(int bitIndex) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void flip(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public boolean get(int bitIndex) {
    return contents.get(bitIndex);
  }

  @Override
  public BitSet get(int fromIndex, int toIndex) {
    return contents.get(fromIndex, toIndex);
  }

  @Override
  public int hashCode() {
    return contents.hashCode();
  }

  @Override
  public boolean intersects(BitSet set) {
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
  public int nextClearBit(int fromIndex) {
    return contents.nextClearBit(fromIndex);
  }

  @Override
  public int nextSetBit(int fromIndex) {
    return contents.nextSetBit(fromIndex);
  }

  @Override
  public void or(BitSet set) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public int previousClearBit(int fromIndex) {
    return contents.previousClearBit(fromIndex);
  }

  @Override
  public int previousSetBit(int fromIndex) {
    return contents.previousSetBit(fromIndex);
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
  public void set(int bitIndex) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void set(int bitIndex, boolean value) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void set(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public void set(int fromIndex, int toIndex, boolean value) {
    throw new UnsupportedOperationException("Read only BitSet");
  }

  @Override
  public int size() {
    return contents.size();
  }

  @Override
  public String toString() {
    return contents.toString();
  }

  @Override
  public void xor(BitSet set) {
    throw new UnsupportedOperationException("Read only BitSet");
  }
}
