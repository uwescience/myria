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
  private transient IPCConnectionPool connectionPool;

  protected final ExchangePairID[] operatorIDs;

  public Producer(final ExchangePairID oID) {
    this(new ExchangePairID[] { oID });
  }

  public Producer(final ExchangePairID[] oIDs) {
    operatorIDs = oIDs;
  }

  public IPCConnectionPool getConnectionPool() {
    return connectionPool;
  }

  public void setConnectionPool(final IPCConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
  }
}
