package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.Server;

public class SQLiteScanEncoding extends OperatorEncoding<SQLiteQueryScan> {
  /** The name of the relation to be scanned. */
  public RelationKey relationKey;
  public Integer storedRelationId;
  private static final List<String> requiredArguments = ImmutableList.of("relationKey");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }

  @Override
  public SQLiteQueryScan construct(final Server server) {
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
    return new SQLiteQueryScan(relationKey, schema);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}