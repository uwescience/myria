package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;

public class NChiladaDatasetEncoding extends MyriaApiEncoding {
  public RelationKey relationKey;
  public String nchiladaDirectoryName;
  public String groupFileName;
  public Set<Integer> workers;
  public List<List<IndexRef>> indexes;
}
