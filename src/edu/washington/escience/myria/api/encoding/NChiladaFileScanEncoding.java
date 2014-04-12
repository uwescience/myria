package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.NChiladaFileScan;
import edu.washington.escience.myria.parallel.Server;

public class NChiladaFileScanEncoding extends LeafOperatorEncoding<NChiladaFileScan> {

  @Required
  public String nchiladaDirectoryName;
  @Required
  public String groupFileName;

  @Override
  public NChiladaFileScan construct(Server server) throws MyriaApiException {
    return new NChiladaFileScan(nchiladaDirectoryName, groupFileName);
  }
}
