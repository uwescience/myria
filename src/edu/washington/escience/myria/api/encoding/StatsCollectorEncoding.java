/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.StatsCollector;
import edu.washington.escience.myria.operator.stats.DataSynopsisSchema;

/**
 * 
 */
public class StatsCollectorEncoding extends
		UnaryOperatorEncoding<StatsCollector> {

	@Required
	public DataSynopsisSchema schema;

	@Override
	public StatsCollector construct(final ConstructArgs args)
			throws MyriaApiException {
		return new StatsCollector(schema, null);
	}

}
