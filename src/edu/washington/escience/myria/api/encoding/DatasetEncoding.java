package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;
import edu.washington.escience.myria.operator.network.distribute.RoundRobinDistributeFunction;

public class DatasetEncoding extends MyriaApiEncoding {
  @Required public RelationKey relationKey;
  @Required public Schema schema;
  public List<Integer> workers;
  @Required public DataSource source;
  public Character delimiter;
  public Character escape;
  public Integer numberOfSkippedLines;
  public Character quote;
  public Boolean importFromDatabase;
  public List<List<IndexRef>> indexes;
  public Boolean overwrite;
  public DistributeFunction distributeFunction = new RoundRobinDistributeFunction();
}
