package edu.washington.escience.myria.parallel;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * The identifier of exchange operators. In a query plan, there may be a set of exchange operators, this ID class is
 * used for the server and the workers to find out which exchange operator is the owner of an arriving
 * {@link ExchangeData}.
 *
 */
public final class ExchangePairID implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The id.
   */
  @JsonProperty private final long oId;

  /**
   * Atomic global id generator.
   * */
  private static final AtomicLong GLOBAL_ID_GENERATOR = new AtomicLong();

  /**
   * Local id generator.
   *
   */
  private static final AtomicLong LOCAL_ID_GENERATOR = new AtomicLong(-1);

  /**
   * @return build one from a long value
   * @param l the long value.
   * */
  public static ExchangePairID fromExisting(final long l) {
    return new ExchangePairID(l);
  }

  /**
   * @return The only way to create a {@link ExchangePairID}.
   */
  public static ExchangePairID newID() {
    return new ExchangePairID(GLOBAL_ID_GENERATOR.getAndIncrement());
  }

  /**
   * A local ID can be used locally in a Worker. It will be used when any worker decides to split some computing load
   * into multiple concurrent running pieces to use multicore.
   *
   * @return new local id.
   * */
  public static ExchangePairID newLocalID() {
    return new ExchangePairID(LOCAL_ID_GENERATOR.getAndDecrement());
  }

  /**
   * for use only in {@link ExchangePairID#fromExisting(long)}.
   *
   * @param oId the long value.
   * */
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

  /**
   * @return the wrapped long value.
   * */
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
