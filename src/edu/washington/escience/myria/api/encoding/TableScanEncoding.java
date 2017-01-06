package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.parallel.Server;

public class TableScanEncoding extends AbstractQueryScanEncoding {
  /** The name of the relation to be scanned. */
  @Required public RelationKey relationKey;
  /**
   * This field is not used by RACO yet but reserved for specifying physical representations of the same logical
   * relation key.
   */
  public Integer storedRelationId;

  public Set<RelationKey> sourceRelationKeys(ConstructArgs args) {
    return ImmutableSet.of(relationKey);
  }

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
