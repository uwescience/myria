package edu.washington.escience.myria.api.encoding;

import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.expression.sql.SqlQuery;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.parallel.Server;

public class QueryScanEncoding extends LeafOperatorEncoding<DbQueryScan> {
  /** The query object that contains everything we need to generate the query. */
  @Required
  public SqlQuery query;
  /** The output schema. */
  @Required
  public Schema schema;

  @Override
  public DbQueryScan construct(final Server server) {
    try {
      query.generateInputSchemas(server);
    } catch (final CatalogException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    }
    return new DbQueryScan(query, schema);
  }
}