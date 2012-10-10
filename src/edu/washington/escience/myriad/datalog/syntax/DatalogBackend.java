package edu.washington.escience.myriad.datalog.syntax;

import java.util.ArrayList;
import java.util.List;

public final class DatalogBackend {
  String fileName;
  String modelName;
  List<DatalogType> modelArgs = new ArrayList<DatalogType>();

  public DatalogBackend(final String model, final List<DatalogType> args) {
    modelName = model;
    modelArgs.addAll(args);
  }

  public DatalogBackend(final String model, final List<DatalogType> args, final String fn) {
    modelName = model;
    modelArgs.addAll(args);
    fileName = fn;
  }

  public void setFilename(final String fn) {
    fileName = fn;
  }

  public String getFilename() {
    return fileName;
  }

  public String getModelName() {
    return modelName;
  }

  public List<DatalogType> getModelArgs() {
    return modelArgs;
  }

  @Override
  public String toString() {
    String res = "backend: ";
    res = res + modelName + "(";
    for (int i = 0; i < modelArgs.size(); i = i + 1) {
      final DatalogType a = modelArgs.get(i);
      res = res + a.toString();
      if (i < (modelArgs.size() - 1)) {
        res = res + ",";
      }
    }
    res = res + ")";
    if (fileName != null) {
      res = res + " [Backed by " + fileName + "]";
    }
    return res;
  }
}