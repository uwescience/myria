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

  public Set<Integer> workers;

  @Override
  public CSVFileScanFragment construct(ConstructArgs args) {
    /* Attempt to use all the workers if not specified */
    if (workers == null) {
      workers = args.getServer().getAliveWorkers();
    }

    /* Find workers */
    int[] workersArray =
        args.getServer().parallelIngestComputeNumWorkers(source.getFileSize(), workers);

    return new CSVFileScanFragment(
        source, reader.getSchema(), workersArray, delimiter, quote, escape, skip);
  }
}
