package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.util.FSUtils;

public class NChiladaDatasetEncoding extends MyriaApiEncoding {
  public RelationKey relationKey;
  public String nchiladaDirectoryName;
  public String groupFileName;
  public Set<Integer> workers;
  public List<List<IndexRef>> indexes;

  @Override
  public void validateExtra() {
    try {
      FSUtils.checkFileReadable(groupFileName);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }
  }
}
