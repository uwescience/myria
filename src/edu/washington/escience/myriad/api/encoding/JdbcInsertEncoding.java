package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.accessmethod.JdbcInfo;
import edu.washington.escience.myriad.operator.JdbcInsert;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Server;

public class JdbcInsertEncoding extends OperatorEncoding<JdbcInsert> {
  /** The name under which the dataset will be stored. */
  public RelationKey relationKey;

  /** The source of tuples to be inserted. */
  public String argChild;

  /** Whether to overwrite an existing dataset. */
  public Boolean argOverwriteTable;

  /** JdbcInfo of this connection. */
  public JdbcInfo jdbcInfo;

  private static final List<String> requiredArguments = ImmutableList.of("relationKey", "argChild", "jdbcInfo");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public JdbcInsert construct(Server server) {
    if (argOverwriteTable != null) {
      return new JdbcInsert(null, relationKey, jdbcInfo, argOverwriteTable);
    }
    return new JdbcInsert(null, relationKey, jdbcInfo);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
