package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

public abstract class Producer extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The worker this operator is located at.
   * 
   */
  private transient Worker thisWorker;

  protected ExchangePairID[] operatorIDs;

  public Producer(final ExchangePairID oID) {
    this(new ExchangePairID[] { oID });
  }

  public Producer(final ExchangePairID[] oIDs) {
    operatorIDs = oIDs;
  }

  public Worker getThisWorker() {
    return thisWorker;
  }

  public void setThisWorker(final Worker thisWorker) {
    this.thisWorker = thisWorker;
  }
}
