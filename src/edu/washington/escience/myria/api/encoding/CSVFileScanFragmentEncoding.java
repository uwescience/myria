package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import edu.washington.escience.myria.CsvTupleReader;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.AmazonS3Source;
import edu.washington.escience.myria.operator.CSVFileScanFragment;

public class CSVFileScanFragmentEncoding extends LeafOperatorEncoding<CSVFileScanFragment> {

  @Required public CsvTupleReader reader;
  @Required public AmazonS3Source source;

  public Character delimiter;
  public Character quote;
  public Character escape;
  public Integer skip;

  private Set<Integer> liveWorkers;

  @Override
  public CSVFileScanFragment construct(ConstructArgs args) {
    /* Attempt to use all the workers */
    liveWorkers = args.getServer().getAliveWorkers();

    return new CSVFileScanFragment(
        source, reader.getSchema(), liveWorkers, delimiter, quote, escape, skip);
  }
}
