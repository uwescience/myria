package edu.washington.escience.myria.api.encoding;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.parallel.Server;

public class TempTableScanEncoding extends LeafOperatorEncoding<DbQueryScan> {
  /** The name of the relation to be scanned. */
  @Required public String table;

  @Override
  public DbQueryScan construct(ConstructArgs args) {
    Schema schema;
    Server server = args.getServer();
    schema = server.getTempSchema(args.getQueryId(), table);
    Preconditions.checkArgument(schema != null, "Specified temp table %s does not exist.", table);
    return new DbQueryScan(RelationKey.ofTemp(args.getQueryId(), table), schema);
  }
}
