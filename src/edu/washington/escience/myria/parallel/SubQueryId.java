package edu.washington.escience.myria.parallel;

import com.google.common.base.Joiner;

/**
 * A struct that holds a composite identifier for a query.
 */
public final class SubQueryId {
  /** The overall query. */
  private final long queryId;
  /** The subquery. */
  private final long subqueryId;

  /**
   * @return the queryId
   */
  public long getQueryId() {
    return queryId;
  }

  /**
   * @return the subqueryId
   */
  public long getSubqueryId() {
    return subqueryId;
  }

  /**
   * Construct the identifier for the corresponding query and subquery.
   *
   * @param queryId the query
   * @param subqueryId the subquery
   */
  public SubQueryId(final long queryId, final long subqueryId) {
    this.queryId = queryId;
    this.subqueryId = subqueryId;
  }

  @Override
  public String toString() {
    return Joiner.on('.').join(queryId, subqueryId);
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    SubQueryId other = (SubQueryId) o;
    return other.queryId == queryId && other.subqueryId == subqueryId;
  }

  @Override
  public int hashCode() {
    return com.google.common.base.Objects.hashCode(queryId, subqueryId);
  }
}
