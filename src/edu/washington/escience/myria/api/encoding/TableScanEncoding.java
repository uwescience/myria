package edu.washington.escience.myria.api.encoding;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.parallel.Server;

public class TableScanEncoding extends LeafOperatorEncoding<DbQueryScan> {
  /** The name of the relation to be scanned. */
  @Required public RelationKey relationKey;
  public Integer storedRelationId;

  @Override
  public DbQueryScan construct(ConstructArgs args) {
    Schema schema;
    Server server = args.getServer();
    try {
      schema = server.getSchema(relationKey);
    } catch (final CatalogException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    }
    Preconditions.checkArgument(
        schema != null, "Specified relation %s does not exist.", relationKey);
    return new DbQueryScan(relationKey, schema);
  }
}
