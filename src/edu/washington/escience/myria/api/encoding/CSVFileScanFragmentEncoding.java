package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.AmazonS3Source;
import edu.washington.escience.myria.operator.CSVFileScanFragment;

public class CSVFileScanFragmentEncoding extends LeafOperatorEncoding<CSVFileScanFragment>{

	  @Required public Schema schema;
	  @Required public AmazonS3Source source;

	  public Character delimiter;
	  public Character quote;
	  public Character escape;
	  public Integer skip;
	  
	  @Required public Boolean readEntireTable; // hack
	  
	  private Set<Integer> liveWorkers; 

	  @Override
	  public CSVFileScanFragment construct(ConstructArgs args) {
		liveWorkers = args.getServer().getAliveWorkers();

	    return new CSVFileScanFragment(source, schema, liveWorkers, readEntireTable, delimiter, quote, escape, skip);
	  }
}
