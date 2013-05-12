package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;

/**
 * A JSON-able wrapper for the expected wire message for a query.
 * 
 */
public class QueryEncoding implements MyriaApiEncoding {
  /** The raw Datalog. */
  public String rawDatalog;
  /** The logical relation algebra plan. */
  public String logicalRa;
  /** The expected number of results (for testing). */
  public Long expectedResultSize;
  /** The query plan encoding. */
  public Map<Integer, List<List<OperatorEncoding<?>>>> queryPlan;

  @Override
  public void validate() throws MyriaApiException {
    try {
      Preconditions.checkNotNull(rawDatalog);
      Preconditions.checkNotNull(logicalRa);
      Preconditions.checkNotNull(queryPlan);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: raw_datalog, logical_ra, query_plan");
    }
  }
}