package edu.washington.escience.myria.api.encoding;

import java.util.List;

import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;
import edu.washington.escience.myria.operator.network.distribute.RoundRobinDistributeFunction;
import edu.washington.escience.myria.util.FSUtils;

public class TipsyDatasetEncoding extends MyriaApiEncoding {
  @Required public RelationKey relationKey;
  @Required public String tipsyFilename;
  @Required public String grpFilename;
  @Required public String iorderFilename;
  public List<Integer> workers;
  public List<List<IndexRef>> indexes;
  public DistributeFunction distributeFunction = new RoundRobinDistributeFunction();

  @Override
  public void validateExtra() {
    /* Note we can only do this because we know that the operator will be run on the master. So we can't do this e.g.
     * for TipsyFileScan because that might be run on a worker. */
    try {
      FSUtils.checkFileReadable(tipsyFilename);
      FSUtils.checkFileReadable(grpFilename);
      FSUtils.checkFileReadable(iorderFilename);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }
  }
}
