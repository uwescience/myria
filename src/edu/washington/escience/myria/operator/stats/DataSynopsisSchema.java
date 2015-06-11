/**
 *
 */
package edu.washington.escience.myria.operator.stats;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Schema for StatsCollector
 */
public class DataSynopsisSchema implements Serializable {
	/** Required for Java serialization */
	private static final long serialVersionUID = 1L;

	@JsonProperty
	public String colName;
	@JsonProperty
	public String algorithm;

	/**
	 * Static factory method.
	 * 
	 * @param algorithm
	 *            data synopsis algorithm
	 * @param colIdx
	 *            which column to collect stats on
	 * @return a Schema representing the specified column types and names.
	 */
	@JsonCreator
	public static DataSynopsisSchema of(
			@JsonProperty(value = "algorithm", required = true) final String algorithm,
			@JsonProperty("columnName") final String colName) {
		return new DataSynopsisSchema(algorithm, colName);
	}

	public int synopsis;

	public DataSynopsisSchema(final String algorithm, final String colName) {
		this.colName = colName;
		switch (algorithm) {
		case "StaticExactBloomFilter":
			synopsis = DataSynopsis.STATIC_EXACT_BLOOM_FILTER;
			break;
		default:
			synopsis = DataSynopsis.NONE;
		}
	}
}
