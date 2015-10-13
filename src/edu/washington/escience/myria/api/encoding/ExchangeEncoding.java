package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import edu.washington.escience.myria.parallel.ExchangePairID;

public interface ExchangeEncoding {

  Set<Integer> getRealWorkerIds();

  void setRealWorkerIds(Set<Integer> w);

  List<ExchangePairID> getRealOperatorIds();

  void setRealOperatorIds(List<ExchangePairID> operatorIds);
}
