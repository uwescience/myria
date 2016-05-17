package edu.washington.escience.myria.api.encoding;

import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.InMemoryOrderBy;

public class InMemoryOrderByEncoding extends UnaryOperatorEncoding<InMemoryOrderBy> {

  @Required public int[] argSortColumns;
  @Required public boolean[] argAscending;

  @Override
  public InMemoryOrderBy construct(final ConstructArgs args) throws MyriaApiException {
    return new InMemoryOrderBy(null, argSortColumns, argAscending);
  }

  @Override
  protected void validateExtra() {
    if (argSortColumns.length != argAscending.length) {
      throw new MyriaApiException(
          Status.BAD_REQUEST, "sort columns number should be equal to ascending orders number!");
    }
  }
}
