package edu.washington.escience.myriad.api.encoding;

import java.util.List;

import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.ExchangePairID;

public abstract class ExchangeEncoding<E extends Operator> extends OperatorEncoding<E> {
  abstract List<String> getOperatorIds();

  private List<Integer> realWorkerIds;
  private List<ExchangePairID> realOperatorIds;

  protected final List<Integer> getRealWorkerIds() {
    return realWorkerIds;
  }

  protected final void setRealWorkerIds(List<Integer> w) {
    realWorkerIds = w;
  }

  protected final List<ExchangePairID> getRealOperatorIds() {
    return realOperatorIds;
  }

  protected final void setRealOperatorIds(List<ExchangePairID> operatorIds) {
    realOperatorIds = operatorIds;
  }
}