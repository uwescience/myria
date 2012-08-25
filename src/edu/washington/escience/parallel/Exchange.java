package edu.washington.escience.parallel;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The exchange operator, which will be used in implementing parallel simpledb.
 * 
 * */
public abstract class Exchange extends Operator {

  /**
   * 
   * The identifier of exchange operators. In a query plan, there may be a set of exchange
   * operators, this ID class is used for the server and the workers to find out which exchange
   * operator is the owner of an arriving ExchangeMessage.
   * 
   * */
  public static class ParallelOperatorID implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * The id
     * */
    private int oId;

    private static final AtomicInteger idGenerator = new AtomicInteger();

    /**
     * The only way to create a ParallelOperatorID.
     * */
    public static ParallelOperatorID newID() {
      return new ParallelOperatorID(idGenerator.getAndIncrement());
    }

    private ParallelOperatorID(int oId) {
      this.oId = oId;
    }

    @Override
    public boolean equals(Object o) {
      ParallelOperatorID oID = (ParallelOperatorID) o;
      if (oID == null)
        return false;
      return oId == oID.oId;
    }

    @Override
    public int hashCode() {
      return this.oId;
    }

    @Override
    public String toString() {
      return oId + "";
    }
  }

  private static final long serialVersionUID = 1L;

  protected final ParallelOperatorID operatorID;

  public Exchange(ParallelOperatorID oID) {
    this.operatorID = oID;
  }

  /**
   * Return the name of the exchange, used only to display the operator in the operator tree
   * */
  public abstract String getName();

  public ParallelOperatorID getOperatorID() {
    return this.operatorID;
  }
}
