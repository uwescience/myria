package edu.washington.escience.myriad.parallel;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The exchange operator, which will be used in implementing parallel simpledb.
 * 
 * */
public abstract class Exchange extends Operator {

  /**
   * 
   * The identifier of exchange operators. In a query plan, there may be a set of exchange operators, this ID class is
   * used for the server and the workers to find out which exchange operator is the owner of an arriving
   * ExchangeMessage.
   * 
   * */
  public static class ExchangePairID implements Serializable {

    /**
     * The id
     * */
    private final long oId;

    private static final AtomicLong idGenerator = new AtomicLong();

    /**
     * The only way to create a ParallelOperatorID.
     * */
    public static ExchangePairID newID() {
      return new ExchangePairID(idGenerator.getAndIncrement());
    }

    public static ExchangePairID fromExisting(long l) {
      return new ExchangePairID(l);
    }

    public long getLong() {
      return this.oId;
    }

    private ExchangePairID(long oId) {
      this.oId = oId;
    }

    @Override
    public boolean equals(Object o) {
      ExchangePairID oID = (ExchangePairID) o;
      if (oID == null)
        return false;
      return oId == oID.oId;
    }

    @Override
    public int hashCode() {
      return (int) this.oId;
    }

    @Override
    public String toString() {
      return oId + "";
    }
  }

  protected final ExchangePairID operatorID;

  public Exchange(ExchangePairID oID) {
    this.operatorID = oID;
  }

  /**
   * Return the name of the exchange, used only to display the operator in the operator tree
   * */
  public abstract String getName();

  public ExchangePairID getOperatorID() {
    return this.operatorID;
  }
}
