package edu.washington.escience.myria.api.encoding;

import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.parallel.Server;

public class TableScanEncoding extends LeafOperatorEncoding<DbQueryScan> {
  /** The name of the relation to be scanned. */
  @Required
  public RelationKey relationKey;
  public Integer storedRelationId;

  @Override
  public DbQueryScan construct(final Server server) {
    Schema schema;
    try {
      schema = server.getSchema(relationKey);
    } catch (final CatalogException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    }
    if (schema == null) {
      throw new MyriaApiException(Status.BAD_REQUEST, "Specified relation "
          + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + " does not exist.");
    }
    return new DbQueryScan(relationKey, schema);
  }

}