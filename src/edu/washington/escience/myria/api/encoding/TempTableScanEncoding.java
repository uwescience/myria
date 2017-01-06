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

public class TempTableScanEncoding extends AbstractQueryScanEncoding {
  /** The name of the relation to be scanned. */
  @Required public String table;

  public Set<RelationKey> sourceRelationKeys(ConstructArgs args) {
    return ImmutableSet.of(RelationKey.ofTemp(args.getQueryId(), table));
  }

  @Override
  public DbQueryScan construct(ConstructArgs args) {
    RelationKey key = RelationKey.ofTemp(args.getQueryId(), table);
    try {
      Schema schema = args.getServer().getSchema(key);
      Preconditions.checkArgument(schema != null, "Specified temp table %s does not exist.", key);
      return new DbQueryScan(key, schema);
    } catch (final CatalogException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
