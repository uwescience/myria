package edu.washington.escience.myriad.parallel;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import edu.washington.escience.myriad.operator.Operator;

/**
 * The exchange operator is an abstract operator used as the foundation for classes that send data over the network.
 * 
 */
public abstract class Exchange extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * 
   * The identifier of exchange operators. In a query plan, there may be a set of exchange operators, this ID class is
   * used for the server and the workers to find out which exchange operator is the owner of an arriving
   * ExchangeMessage.
   * 
   */
  public static final class ExchangePairID implements Serializable {

    /** Required for Java serialization. */
    private static final long serialVersionUID = 1L;

    /**
     * The id
     */
    private final long oId;

    private static final AtomicLong idGenerator = new AtomicLong();

    public static ExchangePairID fromExisting(final long l) {
      return new ExchangePairID(l);
    }

    /**
     * The only way to create a ParallelOperatorID.
     */
    public static ExchangePairID newID() {
      return new ExchangePairID(idGenerator.getAndIncrement());
    }

    private ExchangePairID(final long oId) {
      this.oId = oId;
    }

    @Override
    public boolean equals(final Object o) {
      final ExchangePairID oID = (ExchangePairID) o;
      if (oID == null) {
        return false;
      }
      return oId == oID.oId;
    }

    public long getLong() {
      return oId;
    }

    @Override
    public int hashCode() {
      return (int) oId;
    }

    @Override
    public String toString() {
      return oId + "";
    }
  }

  protected final ExchangePairID operatorID;

  public Exchange(final ExchangePairID oID) {
    operatorID = oID;
  }

  public ExchangePairID getOperatorID() {
    return operatorID;
  }
}
