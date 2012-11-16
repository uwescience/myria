package edu.washington.escience.myriad.parallel;

public abstract class Producer extends Exchange {

  private static final long serialVersionUID = 1L;

  /**
   * The worker this operator is located at.
   * 
   */
  private transient IPCConnectionPool connectionPool;

  public Producer(final ExchangePairID oID) {
    super(oID);
  }

  public IPCConnectionPool getConnectionPool() {
    return connectionPool;
  }

  public void setConnectionPool(final IPCConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
  }

}
