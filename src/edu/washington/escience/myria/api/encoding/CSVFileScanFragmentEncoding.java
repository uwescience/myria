/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.CSVFileScanFragment;

/**
 * 
 */
public class CSVFileScanFragmentEncoding extends LeafOperatorEncoding<CSVFileScanFragment> {
  @Required
  public Schema schema;
  @Required
  public DataSource source;
  public Character delimiter;
  public Character quote;
  public Character escape;
  public Integer skip;
  @Required
  public int workers;

  @Override
  public CSVFileScanFragment construct(final ConstructArgs args) {
    return new CSVFileScanFragment(source, schema, workers, delimiter, quote, escape, skip);
  }
}
