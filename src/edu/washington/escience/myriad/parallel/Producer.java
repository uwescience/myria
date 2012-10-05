package edu.washington.escience.myriad.parallel;

public abstract class Producer extends Exchange {

  private static final long serialVersionUID = 1L;

  /**
   * The worker this operator is located at.
   * 
   * */
  private transient Worker thisWorker;

  public Producer(ExchangePairID oID) {
    super(oID);
  }

  public Worker getThisWorker() {
    return thisWorker;
  }

  public void setThisWorker(Worker thisWorker) {
    this.thisWorker = thisWorker;
  }

}
