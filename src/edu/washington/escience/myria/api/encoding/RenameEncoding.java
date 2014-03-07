package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.operator.Rename;
import edu.washington.escience.myria.parallel.Server;

public class RenameEncoding extends UnaryOperatorEncoding<Rename> {

  @Required
  public List<String> columnNames;

  @Override
  public Rename construct(final Server server) {
    return new Rename(columnNames);
  }
}
