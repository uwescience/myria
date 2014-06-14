package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.io.DataSource;

public class DatasetEncoding extends MyriaApiEncoding {
  @Required
  public RelationKey relationKey;
  @Required
  public Schema schema;
  public Set<Integer> workers;
  @Required
  public DataSource source;
  public Character delimiter;
  public Character escape;
  public Integer numberOfSkippedLines;
  public Character quote;
  public Boolean importFromDatabase;
  public List<List<IndexRef>> indexes;
  public Boolean overwrite;
  public List<Set<Integer>> partitions;
  public Integer numPartitions;
  public Integer repFactor;

  @Override
  protected void validateExtra() throws MyriaApiException {
    if (workers != null) {
      if (partitions != null || numPartitions != null || repFactor != null) {
        throw new MyriaApiException(Status.BAD_REQUEST, "Worker list can be set only if no replication is used.");
      }
    } else {
      if (partitions != null && numPartitions != null) {
        throw new MyriaApiException(Status.BAD_REQUEST,
            "One cannot set both the number of partitions and a partitions list.");
      }
      if (numPartitions == null) {
        /* partition list is set, check upon replication factor */
        if (partitions.size() == 0) {
          throw new MyriaApiException(Status.BAD_REQUEST, "User-specified partitions (optional) cannot be empty.");
        }
        // TODO valmeida traverse the list of partitions validating each component
        numPartitions = partitions.size();
        repFactor = partitions.get(0).size();
      }
      /*
       * It can still happen that the worker list and numPartitions are both null. In this case, we will later construct
       * the partitions with all alive workers.
       */
    }
  }

}
