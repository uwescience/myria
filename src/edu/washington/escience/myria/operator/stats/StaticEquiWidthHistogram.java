/**
 *
 */
package edu.washington.escience.myria.operator.stats;

/**
 * 
 */
public class StaticEquiWidthHistogram extends DataSynopsis {

	/** default number of buckets **/
	private static final int BUCKET_NUM = 1000000;

	private final int[] buckets;

	/** size of each bucket **/
	private final int bucketSize;

	/** lower bound of the value **/
	private final int lower;

	/** upper bound of the value **/
	private final int upper;

	/** number of buckets **/
	private final int bucketNum;

	public StaticEquiWidthHistogram() {
		this(Integer.MIN_VALUE, Integer.MAX_VALUE, BUCKET_NUM);
	}

	public StaticEquiWidthHistogram(final int lower, final int upper,
			final int bucketNum) {
		this.lower = lower;
		this.upper = upper;
		this.bucketNum = bucketNum;
		buckets = new int[bucketNum];
		bucketSize = (upper - lower + bucketNum - 1) / bucketNum;
	}

	@Override
	public void add(final int value) {
		++buckets[getIdx(value)];
	}

	private int getIdx(final int value) {
		return (value - lower + bucketSize) / bucketSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.washington.escience.myria.operator.stats.DataSynopsis#format()
	 */
	@Override
	public String format() {
		// TODO Auto-generated method stub
		return null;
	}
}
