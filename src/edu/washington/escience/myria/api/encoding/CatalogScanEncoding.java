package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.CatalogQueryScan;

public class CatalogScanEncoding extends LeafOperatorEncoding<CatalogQueryScan> {
  @Required
  public Schema schema;
  @Required
  public String sql;

  @Override
  public CatalogQueryScan construct(@Nonnull final ConstructArgs args) {
    return new CatalogQueryScan(sql, schema, args.getServer().getCatalog());
  }

}