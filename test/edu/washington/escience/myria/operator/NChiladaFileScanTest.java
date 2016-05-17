package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.EqualsExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.VariableExpression;

public class NChiladaFileScanTest {

  private static final String[] STAR_DIR_FILE_NAMES = {
    "ENSRate",
    "FeMassFrac",
    "OxMassFracDot",
    "den",
    "iord",
    "mass",
    "metals",
    "pos",
    "pot",
    "smoothlength",
    "soft",
    "vel",
    "igasord",
    "massform",
    "tform"
  };
  private static final String[] DARK_DIR_FILE_NAMES = {
    "den", "iord", "mass", "pos", "pot", "smoothlength", "soft", "vel"
  };
  private static final String[] GAS_DIR_FILE_NAMES = {
    "ESNRate",
    "FeMassFrac",
    "FeMassFracdot",
    "GasDensity",
    "HI",
    "HeI",
    "HeII",
    "Metalsdot",
    "OxMassFrac",
    "OxMassFracdot",
    "coolontime",
    "den",
    "iord",
    "mass",
    "metals",
    "pos",
    "pot",
    "smoothlength",
    "soft",
    "temperature",
    "vel"
  };
  /** The star directory name. */
  private static final String STAR_DIR = "/star";
  /** The dark directory name. */
  private static final String DARK_DIR = "/dark";
  /** The gas directory name. */
  private static final String GAS_DIR = "/gas";

  @Test
  public void testSimpleFile()
      throws NoSuchMethodException, SecurityException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException,
          FileNotFoundException, DbException {
    NChiladaFileScan fileScan = getNChiladaFileScanSimple();
    assertEquals(9, TipsyFileScanTest.getRowCount(fileScan));
  }

  @Test
  public void testSimpleGas()
      throws DbException, NoSuchMethodException, SecurityException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException,
          FileNotFoundException {
    testSimple("gas");
  }

  @Test
  public void testSimpleDark()
      throws DbException, NoSuchMethodException, SecurityException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException,
          FileNotFoundException {
    testSimple("dark");
  }

  @Test
  public void testSimpleStar()
      throws DbException, NoSuchMethodException, SecurityException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException,
          FileNotFoundException {
    testSimple("star");
  }

  private void testSimple(String filterType)
      throws DbException, NoSuchMethodException, SecurityException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException,
          FileNotFoundException {
    NChiladaFileScan fileScan = getNChiladaFileScanSimple();
    ExpressionOperator expr =
        new EqualsExpression(
            new VariableExpression(29), new ConstantExpression(Type.STRING_TYPE, filterType));
    Filter filter = new Filter(new Expression(null, expr), fileScan);
    assertEquals(3, TipsyFileScanTest.getRowCount(filter));
  }

  private NChiladaFileScan getNChiladaFileScanSimple()
      throws NoSuchMethodException, SecurityException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException,
          FileNotFoundException {
    String dir =
        "testdata" + File.separatorChar + "nchiladafilescan" + File.separatorChar + "testsimple";
    String groupFile =
        "testdata" + File.separatorChar + "nchiladafilescan" + File.separatorChar + "grpFile1";
    Constructor<NChiladaFileScan> c =
        NChiladaFileScan.class
            .getDeclaredConstructor(InputStream.class, Map.class, Map.class, Map.class);
    c.setAccessible(true);
    NChiladaFileScan fileScan =
        c.newInstance(
            getGroupFileStream(groupFile),
            getFilesToDataInput(dir + GAS_DIR),
            getFilesToDataInput(dir + STAR_DIR),
            getFilesToDataInput(dir + DARK_DIR));
    return fileScan;
  }

  private InputStream getGroupFileStream(String groupFilePath) throws FileNotFoundException {
    return new FileInputStream(groupFilePath);
  }

  private Map<String, DataInput> getFilesToDataInput(String path) throws FileNotFoundException {
    File dir = new File(path);
    if (!dir.exists() || !dir.isDirectory()) {
      throw new IllegalArgumentException("Invalid directory: " + path);
    }
    Map<String, DataInput> map = new HashMap<>();
    for (String fileName : dir.list()) {
      DataInput dataInputStream =
          new DataInputStream(
              new BufferedInputStream(
                  new FileInputStream(dir.getPath() + File.separatorChar + fileName)));
      map.put(fileName, dataInputStream);
    }
    return map;
  }

  @SuppressWarnings("unused")
  // It was used to generate test data, keep it here for posterity
  private static void generateNChiladaFiles(String dirName, int records) throws IOException {
    String starDirPath = dirName + "/star";
    String darkDirPath = dirName + "/dark";
    String gasDirPath = dirName + "/gas";
    File starDir = new File(starDirPath);
    starDir.mkdir();
    File gasDir = new File(gasDirPath);
    gasDir.mkdir();
    File darkDir = new File(darkDirPath);
    darkDir.mkdir();
    DataOutputStream[] starFilesOutputStreams = new DataOutputStream[STAR_DIR_FILE_NAMES.length];
    DataOutputStream[] darkFilesOutputStreams = new DataOutputStream[DARK_DIR_FILE_NAMES.length];
    DataOutputStream[] gasFilesOutputStreams = new DataOutputStream[GAS_DIR_FILE_NAMES.length];
    populateOutputStreamsArray(starFilesOutputStreams, STAR_DIR_FILE_NAMES, starDirPath);
    populateOutputStreamsArray(darkFilesOutputStreams, DARK_DIR_FILE_NAMES, darkDirPath);
    populateOutputStreamsArray(gasFilesOutputStreams, GAS_DIR_FILE_NAMES, gasDirPath);
    outputData(starFilesOutputStreams, STAR_DIR_FILE_NAMES, records);
    outputData(darkFilesOutputStreams, DARK_DIR_FILE_NAMES, records);
    outputData(gasFilesOutputStreams, GAS_DIR_FILE_NAMES, records);
  }

  private static void populateOutputStreamsArray(
      DataOutputStream[] array, String[] fileNames, String path) throws FileNotFoundException {
    for (int i = 0; i < fileNames.length; i++) {
      FileOutputStream fStream = new FileOutputStream(path + File.separatorChar + fileNames[i]);
      array[i] = new DataOutputStream(fStream);
    }
  }

  private static void outputData(DataOutputStream[] array, String[] fileNames, int records)
      throws IOException {
    for (int i = 0; i < array.length; i++) {
      String fileName = fileNames[i];
      DataOutputStream dataOutput = array[i];
      // Write header.
      dataOutput.writeInt(1062053); // magic
      dataOutput.writeDouble(0); // time
      dataOutput.writeInt(0); // iHighWord
      dataOutput.writeInt(records); // nbodies
      if (fileName.equals("vel") || fileName.equals("pos")) {
        dataOutput.writeInt(3); // ndim
      } else {
        dataOutput.writeInt(1); // ndim
      }
      if (fileName.equals("iord") || fileName.equals("igasord")) {
        dataOutput.writeInt(5); // code
        dataOutput.writeInt(0); // min value
        dataOutput.writeInt(0); // max value
      } else {
        dataOutput.writeInt(9); // code
        dataOutput.writeFloat(0); // min value
        dataOutput.writeFloat(0); // max value
      }
      for (int j = 0; j < records; j++) {
        if (fileName.equals("vel") || fileName.equals("pos")) {
          dataOutput.writeFloat(i + j);
          dataOutput.writeFloat(i + j);
          dataOutput.writeFloat(i + j);
        } else if (fileName.equals("iord") || fileName.equals("igasord")) {
          dataOutput.writeInt(i + j);
        } else {
          dataOutput.writeFloat(i + j);
        }
      }
      dataOutput.close();
    }
  }
}
