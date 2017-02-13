/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.io.AmazonS3Source;
import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;
import edu.washington.escience.myria.operator.network.distribute.RoundRobinDistributeFunction;

/**
 *
 */
public class ParallelDatasetEncoding extends MyriaApiEncoding {
  @Required public RelationKey relationKey;
  @Required public Schema schema;
  @Required public AmazonS3Source s3Source;
  public Character delimiter;
  public Character escape;
  public Integer numberOfSkippedLines;
  public Character quote;
  public Set<Integer> workers;
  public DistributeFunction distributeFunction = new RoundRobinDistributeFunction();
}
