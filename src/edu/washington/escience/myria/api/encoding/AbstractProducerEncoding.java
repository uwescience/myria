package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import edu.washington.escience.myria.operator.network.Producer;
import edu.washington.escience.myria.parallel.ExchangePairID;

public abstract class AbstractProducerEncoding<P extends Producer> extends UnaryOperatorEncoding<P>
    implements ExchangeEncoding {
  private Set<Integer> realWorkerIds;
  private List<ExchangePairID> realOperatorIds;

  @Override
  public final Set<Integer> getRealWorkerIds() {
    return realWorkerIds;
  }

  @Override
  public final void setRealWorkerIds(Set<Integer> w) {
    realWorkerIds = w;
  }

  @Override
  public final List<ExchangePairID> getRealOperatorIds() {
    return realOperatorIds;
  }

  @Override
  public final void setRealOperatorIds(List<ExchangePairID> operatorIds) {
    realOperatorIds = operatorIds;
  }
}
