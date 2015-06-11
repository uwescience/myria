/**
 *
 */
package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.stats.DataSynopsis;
import edu.washington.escience.myria.operator.stats.DataSynopsisException;
import edu.washington.escience.myria.operator.stats.DataSynopsisSchema;
import edu.washington.escience.myria.operator.stats.StaticExactBloomFilter;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * 
 */
public class StatsCollector extends UnaryOperator {

	/** Required for serialization */
	private static final long serialVersionUID = 1L;

	private final DataSynopsisSchema statsSchema;
	private DataSynopsis synopsis;
	private int colIdx;

	/**
	 * @param schema
	 *            the schema of the stats collector
	 * @param child
	 *            the source of tuples
	 */
	public StatsCollector(final DataSynopsisSchema schema, final Operator child) {
		super(child);
		statsSchema = schema;
	}

	@Override
	protected TupleBatch fetchNextReady() throws DbException {
		Operator child = getChild();
		TupleBatch tb = child.nextReady();
		if (tb != null) {
			for (int rowIdx = 0; rowIdx < tb.numTuples(); rowIdx++) {
				synopsis.add(tb.getInt(colIdx, rowIdx));
			}
			if (tb.isEOI()) {
				System.out.println(synopsis.format());
			}
		}
		return tb;
	}

	@Override
	protected void init(final ImmutableMap<String, Object> execEnvVars)
			throws DataSynopsisException {

		/** check the column is integer */
		Schema inputSchema = getChild().getSchema();

		colIdx = inputSchema.columnNameToIndex(statsSchema.colName);
		if (!inputSchema.getColumnType(colIdx).equals(Type.INT_TYPE)) {
			throw new DataSynopsisException("Column " + colIdx
					+ " is not an integer");
		}
		switch (statsSchema.synopsis) {
		case (DataSynopsis.STATIC_EXACT_BLOOM_FILTER):
			synopsis = new StaticExactBloomFilter();
			break;
		default:
			synopsis = null;
		}

	}

	@Override
	public Schema generateSchema() {
		Operator child = getChild();
		if (child == null) {
			return null;
		}
		return child.getSchema();
	}
}
