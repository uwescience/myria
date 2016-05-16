package edu.washington.escience.myria.parallel.ipc;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;

/**
 * A set of channels. Ordered by a comparator.
 * */
public final class ChannelPrioritySet {

  /**
   * Snapshot iterator that works off copy of underlying q array.
   */
  final class Itr implements Iterator<Channel> {
    /** Array of all elements. */
    private final Object[] array;
    /** Index of next element to return. */
    private int cursor;

    /**
     * @param array the data array.
     * */
    Itr(final Object[] array) {
      this.array = array;
    }

    @Override
    public boolean hasNext() {
      return cursor < array.length;
    }

    @Override
    public Channel next() {
      if (cursor >= array.length) {
        throw new NoSuchElementException();
      }
      return (Channel) array[cursor++];
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * guard the modification of this data structure.
   * */
  private final Lock updateLock = new ReentrantLock();

  /**
   * Expose update lock for deadlock removing.
   *
   * @return update lock
   * */
  Lock getUpdateLock() {
    return updateLock;
  }

  /**
   * size restriction, lower bound.
   * */
  private final int lowerBound;
  /**
   * size restriction, upper bound.
   * */
  private final int upperBound;
  /**
   * using PriorityQueue for ordering.
   * */
  private final PriorityQueue<Channel> orderedChannels;

  /**
   * using HashSet to keep elements unique.
   * */
  private final HashSet<Channel> set;

  /**
   * Creates a {@code PriorityBlockingQueue} with the specified initial capacity that orders its elements according to
   * the specified comparator.
   *
   * @param initialCapacity the initial capacity for this priority queue
   * @param comparator the comparator that will be used to order this priority queue. If {@code null}, the
   *          {@linkplain Comparable natural ordering} of the elements will be used.
   * @param upperBound channel pool upper bound
   * @param lowerBound channel pool lower bound
   * @throws IllegalArgumentException if {@code initialCapacity} is less than 1
   */
  public ChannelPrioritySet(
      final int initialCapacity,
      final int lowerBound,
      final int upperBound,
      final Comparator<? super Channel> comparator) {
    orderedChannels = new PriorityQueue<Channel>(initialCapacity, comparator);
    set = new HashSet<Channel>(initialCapacity);
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  /**
   * Inserts.
   *
   * @param e the channel to add
   * @return {@code true} (as specified by {@link Collection#add})
   * @throws ClassCastException if the specified element cannot be compared with elements currently in the priority
   *           queue according to the priority queue's ordering
   * @throws NullPointerException if the specified element is null
   */
  public boolean add(final Channel e) {
    updateLock.lock();
    try {
      if (set.add(e)) {
        return orderedChannels.add(e);
      } else {
        return false;
      }
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * @return a read-only collection of all channels within this set.
   * */
  public Collection<Channel> allChannels() {
    return Collections.unmodifiableCollection(orderedChannels);
  }

  /**
   * Returns an iterator over the elements in this queue. The iterator does not return the elements in any particular
   * order.
   *
   * <p>
   * The returned iterator is a "weakly consistent" iterator that will never throw
   * {@link java.util.ConcurrentModificationException ConcurrentModificationException}, and guarantees to traverse
   * elements as they existed upon construction of the iterator, and may (but is not guaranteed to) reflect any
   * modifications subsequent to construction.
   *
   * @return an iterator over the elements in this queue
   */
  public Iterator<Channel> iterator() {
    return new Itr(toArray());
  }

  /**
   * peek and reserve the channel, so that the channel will never be disconnected.
   *
   * @return the top channel .
   * */
  public Channel peekAndReserve() {

    updateLock.lock();
    try {
      final Channel cc = orderedChannels.peek();
      if (cc != null) {
        final ChannelContext bc = ChannelContext.getChannelContext(cc);
        (bc.getRegisteredChannelContext()).incReference();
      }
      return cc;
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * release a channel.
   *
   * @param ch channel.
   * @param recyclableConnections collection of recyclable connections
   * @param trash the trash bin.
   * */
  public void release(
      final Channel ch,
      final ChannelGroup trash,
      final ConcurrentHashMap<Channel, Channel> recyclableConnections) {
    final ChannelContext cc = ChannelContext.getChannelContext(ch);
    final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();

    updateLock.lock();
    try {
      final int size = size();
      final int currentReferenced = ecc.decReference();
      if (currentReferenced <= 0) {
        if (size > upperBound) {
          // release
          cc.reachUpperbound(trash, this);
        } else if (size > lowerBound) {
          // wait for timeout then release
          cc.considerRecycle(recyclableConnections);
        }
      }
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * Removes a single instance of the specified element from this queue, if it is present. More formally, removes an
   * element {@code e} such that {@code o.equals(e)}, if this queue contains one or more such elements. Returns
   * {@code true} if and only if this queue contained the specified element (or equivalently, if this queue changed as a
   * result of the call).
   *
   * @param o element to be removed from this queue, if present
   * @return {@code true} if this queue changed as a result of the call
   */
  public boolean remove(final Channel o) {
    updateLock.lock();
    try {
      if (set.remove(o)) {
        return orderedChannels.remove(o);
      } else {
        return false;
      }
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * @return size of this channel set.
   * */
  public int size() {
    updateLock.lock();
    try {
      return set.size();
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * @return an array representation of all the channels in this set.
   * */
  public Object[] toArray() {
    updateLock.lock();
    try {
      return orderedChannels.toArray();
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * Returns an array containing all of the elements in this queue; the runtime type of the returned array is that of
   * the specified array. The returned array elements are in no particular order. If the queue fits in the specified
   * array, it is returned therein. Otherwise, a new array is allocated with the runtime type of the specified array and
   * the size of this queue.
   *
   * <p>
   * If this queue fits in the specified array with room to spare (i.e., the array has more elements than this queue),
   * the element in the array immediately following the end of the queue is set to {@code null}.
   *
   * <p>
   * Like the {@link #toArray()} method, this method acts as bridge between array-based and collection-based APIs.
   * Further, this method allows precise control over the runtime type of the output array, and may, under certain
   * circumstances, be used to save allocation costs.
   *
   * <p>
   * Suppose {@code x} is a queue known to contain only strings. The following code can be used to dump the queue into a
   * newly allocated array of {@code String}:
   *
   * <pre>
   *     String[] y = x.toArray(new String[0]);</pre>
   *
   * Note that {@code toArray(new Object[0])} is identical in function to {@code toArray()}.
   *
   * @param a the array into which the elements of the queue are to be stored, if it is big enough; otherwise, a new
   *          array of the same runtime type is allocated for this purpose
   * @return an array containing all of the elements in this queue
   * @throws ArrayStoreException if the runtime type of the specified array is not a supertype of the runtime type of
   *           every element in this queue
   * @throws NullPointerException if the specified array is null
   * @param <T> the type of the array.
   */
  public <T> T[] toArray(final T[] a) {
    updateLock.lock();
    try {
      return orderedChannels.toArray(a);
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public String toString() {
    updateLock.lock();
    try {
      return orderedChannels.toString();
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * Update the priority value of element e.
   *
   * It's implemented as first remove e from queue and then add e back. If e is not in queue, simply add it to queue.
   *
   * @param e element.
   * @return true if this queue changed.
   * */
  public boolean update(final Channel e) {
    updateLock.lock();
    try {
      if (remove(e)) {
        return add(e);
      } else {
        return false;
      }
    } finally {
      updateLock.unlock();
    }
  }
}
