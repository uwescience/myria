/**
 *
 */
package edu.washington.escience.myria.operator.stats;

/**
 * 
 */
public abstract class DataSynopsis {
	public final static int NONE = 0;
	public final static int STATIC_EXACT_BLOOM_FILTER = 1;

	public abstract void add(int key);

	public abstract String format();
}
