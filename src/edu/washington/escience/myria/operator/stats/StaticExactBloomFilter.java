/**
 *
 */
package edu.washington.escience.myria.operator.stats;

import java.util.BitSet;

/**
 * A static exact bloom filter for integers from -2^31 to 2^31
 */
public class StaticExactBloomFilter extends DataSynopsis {
	/** Required for Java serialization. */
	private static final long serialVersionUID = 1L;

	/** lower bound of the bloom filter */
	private final int lower;
	/** upper bound of the bloom filter */
	private final int upper;
	/** number of bits */
	private final int size;
	/** size of a bucket */
	private final int bucketSize;
	private final BitSet bits;

	/** default number of bits **/
	private final static int BIT_NUM = 1000000000;

	public StaticExactBloomFilter() {
		// the default size of the bloom filter is 1M
		this(Integer.MIN_VALUE, Integer.MAX_VALUE, BIT_NUM);
	}

	public StaticExactBloomFilter(final int lower, final int upper,
			final int size) {
		this.lower = lower;
		this.upper = upper;
		this.size = size;
		bucketSize = (upper - lower + size - 1) / size;
		bits = new BitSet(size);
	}

	@Override
	public void add(final int key) {
		bits.set(getIdx(key));
	}

	@Override
	public String format() {
		String str = "StaticExactBloomFilter: ";
		str += lower + ", " + upper + ", " + size;
		return str;
	}

	private int getIdx(final int key) {
		return (key + bucketSize - 1) / bucketSize;
	}
}
