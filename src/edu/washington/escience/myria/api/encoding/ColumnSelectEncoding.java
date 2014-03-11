package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.operator.ColumnSelect;
import edu.washington.escience.myria.parallel.Server;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 * @author leelee
 * 
 */
public class ColumnSelectEncoding extends UnaryOperatorEncoding<ColumnSelect> {

  @Required
  public int[] argFieldList;

  @Override
  public ColumnSelect construct(Server server) {
    try {
      return new ColumnSelect(argFieldList, null);
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }
}
