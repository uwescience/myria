package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import edu.washington.escience.myria.CsvTupleReader;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.AmazonS3Source;
import edu.washington.escience.myria.operator.CSVFragmentTupleSource;

public class CSVFragmentTupleSourceEncoding extends LeafOperatorEncoding<CSVFragmentTupleSource> {

  @Required public CsvTupleReader reader;
  @Required public AmazonS3Source source;

  public Set<Integer> workers;

  @Override
  public CSVFragmentTupleSource construct(ConstructArgs args) {
    /* Attempt to use all the workers if not specified */
    if (workers == null) {
      workers = args.getServer().getAliveWorkers();
    }

    /* Find workers */
    int[] workersArray =
        args.getServer().parallelIngestComputeNumWorkers(source.getFileSize(), workers);

    return new CSVFragmentTupleSource(
        source,
        reader.getSchema(),
        workersArray,
        reader.getDelimiter(),
        reader.getQuote(),
        reader.getEscape(),
        reader.getSkip());
  }
}
