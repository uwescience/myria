package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.NChiladaFileScan;

public class NChiladaFileScanEncoding extends LeafOperatorEncoding<NChiladaFileScan> {

  @Required
  public String nchiladaDirectoryName;
  @Required
  public String groupFileName;

  @Override
  public NChiladaFileScan construct(@Nonnull ConstructArgs args) throws MyriaApiException {
    return new NChiladaFileScan(nchiladaDirectoryName, groupFileName);
  }
}
