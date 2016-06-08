package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;

public class DatasetEncoding extends MyriaApiEncoding {
  @Required public RelationKey relationKey;
  @Required public Schema schema;
  public Set<Integer> workers;
  @Required public DataSource source;
  public Character delimiter;
  public Character escape;
  public Integer numberOfSkippedLines;
  public Character quote;
  public Boolean importFromDatabase;
  public List<List<IndexRef>> indexes;
  public Boolean overwrite;
  public PartitionFunction partitionFunction = new RoundRobinPartitionFunction(null);
}
