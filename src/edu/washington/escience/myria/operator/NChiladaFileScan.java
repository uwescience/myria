package edu.washington.escience.myria.operator;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * Parse NChilada file formats. See <a
 * href="http://librarian.phys.washington.edu/astro/index.php/Research:NChilada_File_Format">NChilada wiki</a>
 *
 * @author leelee
 *
 */
public class NChiladaFileScan extends LeafOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** IOrder attribute that exists in all three types of particles. */
  private static final String IORD = "iord";
  /** Den attribute that exists in all three types of particles. */
  private static final String DEN = "den";
  /** Mass attribute that exists in all three types of particles. */
  private static final String MASS = "mass";
  /** Position x attribute that exists in all three types of particles. */
  private static final String POS_X = "x";
  /** Position y attribute that exists in all three types of particles. */
  private static final String POS_Y = "y";
  /** Position z attribute that exists in all three types of particles. */
  private static final String POS_Z = "z";
  /** Position attribute file name. */
  private static final String POS_FILE_NAME = "pos";
  /** Pot attribute that exists in all three types of particles. */
  private static final String POT = "pot";
  /** Smoothlength attribute that exists in all three types of particles. */
  private static final String SMOOTHLENGTH = "smoothlength";
  /** Soft attribute that exists in all three types of particles. */
  private static final String SOFT = "soft";
  /** Velocity x attribute that exists in all three types of particles. */
  private static final String VEL_X = "vx";
  /** Velocity y attribute that exists in all three types of particles. */
  private static final String VEL_Y = "vy";
  /** Velocity z attribute that exists in all three types of particles. */
  private static final String VEL_Z = "vz";
  /** Position attribute file name. */
  private static final String VEL_FILE_NAME = "vel";

  /** Gas iOrder attribute that only exists in star particles. */
  private static final String IGASORD = "igasord";
  /** Massform attribute that only exists in star particles. */
  private static final String MASSFORM = "massform";
  /** Tform attribute that only exists in star particles. */
  private static final String TFORM = "tform";

  /** ESNRate attribute that only exists in gas and star particles. */
  private static final String ESN_RATE = "ESNRate";
  /** FeMassFrac attribute that only exists in gas and star particles. */
  private static final String FE_MASS_FRAC = "FeMassFrac";
  /** OxMassFrac attribute that only exists in gas and star particles. */
  private static final String OX_MASS_FRAC = "OxMassFrac";
  /** Metals attribute that only exists in gas and star particles. */
  private static final String METALS = "metals";

  /** FeMassFracDot attribute that only exists in gas particles. */
  private static final String FE_MASS_FRACDOT = "FeMassFracDot";
  /** GasDensity attribute that only exists in gas particles. */
  private static final String GAS_DENSITY = "GasDensity";
  /** HI attribute that only exists in gas particles. */
  private static final String H_I = "HI";
  /** HeI attribute that only exists in gas particles. */
  private static final String HE_I = "HeI";
  /** HeII attribute that only exists in gas particles. */
  private static final String HE_I_I = "HeII";
  /** Metalsdot attribute that only exists in gas particles. */
  private static final String METALSDOT = "Metalsdot";
  /** OxMassFracdot attribute that only exists in gas particles. */
  private static final String OX_MASS_FRACDOT = "OxMassFracdot";
  /** Coolontime attribute that only exists in gas particles. */
  private static final String COOLONTIME = "coolontime";
  /** Temperature attribute that only exists in gas particles. */
  private static final String TEMPERATURE = "temperature";

  /** The star directory name. */
  private static final String STAR_DIR = "/star";
  /** The dark directory name. */
  private static final String DARK_DIR = "/dark";
  /** The gas directory name. */
  private static final String GAS_DIR = "/gas";

  /** The column types for NChilada schema. */
  private static final List<Type> NCHILADA_COLUMN_TYPES =
      ImmutableList.of(
          Type.INT_TYPE,
          Type.INT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.FLOAT_TYPE,
          Type.INT_TYPE,
          Type.STRING_TYPE);
  /** The column names for NChilada schema. */
  private static final List<String> NCHILADA_COLUMN_NAMES =
      ImmutableList.of(
          IORD,
          IGASORD,
          ESN_RATE,
          FE_MASS_FRAC,
          FE_MASS_FRACDOT,
          GAS_DENSITY,
          H_I,
          HE_I,
          HE_I_I,
          METALSDOT,
          OX_MASS_FRAC,
          OX_MASS_FRACDOT,
          COOLONTIME,
          DEN,
          MASS,
          METALS,
          POS_X,
          POS_Y,
          POS_Z,
          POT,
          SMOOTHLENGTH,
          SOFT,
          VEL_X,
          VEL_Y,
          VEL_Z,
          MASSFORM,
          TFORM,
          TEMPERATURE,
          "grp",
          "type");
  /** Schema for all NChilada files. */
  private static final Schema NCHILADA_SCHEMA =
      new Schema(NCHILADA_COLUMN_TYPES, NCHILADA_COLUMN_NAMES);

  /** The magic number that indicates the file format is NChilada. */
  private static final int NCHILADA_FORMAT = 1062053;
  /** The number of dimension that vel and pos in NChilada file format should have. */
  private static final int VEL_POS_DIM = 3;
  /** The code that indicates the data type of the file is float. */
  private static final int FLOAT_CODE = 9;
  /** The code that indicates the data type of the file is int. */
  private static final int INT_CODE = 5;
  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;

  /** The full path of the directory that contains star, dark and gas directories. */
  private String particleDirectoryPath;
  /** The full path of the file that contains groupNumber in the order of gas, dark, star. */
  private String groupFilePath;
  /** The group input stream. */
  private InputStream groupInputStream;
  /** Contains matching from file name to DataInput object for star particles attributes. */
  private Map<String, DataInput> starAttributeFilesToDataInput;
  /** Contains matching from file name to DataInput object for gas particles attributes. */
  private Map<String, DataInput> gasAttributeFilesToDataInput;
  /** Contains matching from file name to DataInput object for dark particles attributes. */
  private Map<String, DataInput> darkAttributeFilesToDataInput;
  /** The number of star particle records. */
  private int numStar;
  /** The number of gas particle records. */
  private int numGas;
  /** The number of dark particle records. */
  private int numDark;
  /** Scanner used to parse the group number file. */
  private transient Scanner groupScanner;
  /** Which line of the file the scanner is currently on. */
  private int lineNumber;

  /**
   * Represents different types of particle.
   */
  private enum ParticleType {
    /** There are three types of particles. */
    DARK,
    GAS,
    STAR
  }

  /**
   * Construct a new NChiladaFileScanObject.
   *
   * @param particleDirectoryPath The full path of the directory that contains gas, star, dark directories.
   * @param groupFilePath The full path of the file that contains groupNumber in the order of gas, dark, star.
   */
  public NChiladaFileScan(final String particleDirectoryPath, final String groupFilePath) {
    Objects.requireNonNull(particleDirectoryPath);
    Objects.requireNonNull(groupFilePath);
    this.particleDirectoryPath = particleDirectoryPath;
    this.groupFilePath = groupFilePath;
  }

  /**
   * Construct a new NChiladaFileScanObject. This constructor is only meant to be called from test.
   *
   * @param groupInputStream The InputStream object for group.
   * @param gasAttributeFilesToDataInput A mapping from gas attribute file names to their respective DataInput object.
   * @param starAttributeFilesToDataInput A mapping from star attribute file names to their respective DataInput object.
   * @param darkAttributeFilesToDataInput A mapping from dark attribute file names to their respective DataInput object.
   */
  @SuppressWarnings("unused")
  // used via reflection in the tests
  private NChiladaFileScan(
      final InputStream groupInputStream,
      final Map<String, DataInput> gasAttributeFilesToDataInput,
      final Map<String, DataInput> starAttributeFilesToDataInput,
      final Map<String, DataInput> darkAttributeFilesToDataInput) {
    Objects.requireNonNull(groupInputStream);
    Objects.requireNonNull(gasAttributeFilesToDataInput);
    Objects.requireNonNull(starAttributeFilesToDataInput);
    Objects.requireNonNull(darkAttributeFilesToDataInput);
    this.darkAttributeFilesToDataInput = darkAttributeFilesToDataInput;
    this.gasAttributeFilesToDataInput = gasAttributeFilesToDataInput;
    this.starAttributeFilesToDataInput = starAttributeFilesToDataInput;
    this.groupInputStream = groupInputStream;
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    processFile(ParticleType.GAS);
    processFile(ParticleType.DARK);
    processFile(ParticleType.STAR);
    return buffer.popAny();
  }

  /**
   * Create InputStream object for the given group file path.
   *
   * @param groupFilePath The file path to create InputStream object.
   * @return the group InputStream object.
   * @throws DbException The DbException.
   */
  private InputStream getGroupFileStream(final String groupFilePath) throws DbException {
    InputStream groupInputStreamLocal;
    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(URI.create(groupFilePath), conf);
      Path rootPath = new Path(groupFilePath);
      groupInputStreamLocal = fs.open(rootPath);
    } catch (IOException e) {
      throw new DbException(e);
    }
    return groupInputStreamLocal;
  }

  /**
   * Create DataInput object for every files in the directory.
   *
   * @param path The directory path.
   * @return a mapping from filename to DataInput object.
   * @throws DbException The DbException.
   */
  private Map<String, DataInput> getFilesToDataInput(final String path) throws DbException {
    Configuration conf = new Configuration();
    FileSystem fs;
    Map<String, DataInput> map = new HashMap<>();
    try {
      fs = FileSystem.get(URI.create(path), conf);
      Path rootPath = new Path(path + File.separator);
      FileStatus[] statii = fs.listStatus(rootPath);
      if (statii == null || statii.length == 0) {
        throw new FileNotFoundException(path);
      }
      for (FileStatus status : statii) {
        Path p = status.getPath();
        String[] pNameTokens = p.getName().split(Pattern.quote(File.separator));
        String fileName = pNameTokens[pNameTokens.length - 1];
        DataInput dataInputStream = fs.open(p);
        map.put(fileName, dataInputStream);
      }
    } catch (IOException e) {
      throw new DbException(e);
    }
    return map;
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    numDark = -1;
    numGas = -1;
    numStar = -1;
    if (darkAttributeFilesToDataInput == null) {
      darkAttributeFilesToDataInput = getFilesToDataInput(particleDirectoryPath + DARK_DIR);
    }
    if (gasAttributeFilesToDataInput == null) {
      gasAttributeFilesToDataInput = getFilesToDataInput(particleDirectoryPath + GAS_DIR);
    }
    if (starAttributeFilesToDataInput == null) {
      starAttributeFilesToDataInput = getFilesToDataInput(particleDirectoryPath + STAR_DIR);
    }
    Preconditions.checkArgument(
        darkAttributeFilesToDataInput != null, "darkAttributeFilesToDataInput has not been set");
    Preconditions.checkArgument(
        starAttributeFilesToDataInput != null, "starAttributeFilesToDataInput has not been set");
    Preconditions.checkArgument(
        gasAttributeFilesToDataInput != null, "gasAttributeFilesToDataInput has not been set");
    buffer = new TupleBatchBuffer(getSchema());
    initBasedOnParticleType(ParticleType.GAS);
    initBasedOnParticleType(ParticleType.DARK);
    initBasedOnParticleType(ParticleType.STAR);
    if (groupInputStream == null) {
      groupInputStream = getGroupFileStream(groupFilePath);
    }
    Preconditions.checkArgument(
        groupInputStream != null, "FileScan group input stream has not been set.");
    groupScanner = new Scanner(new BufferedReader(new InputStreamReader(groupInputStream)));
    int numGroup = groupScanner.nextInt();
    int numTot = numGas + numDark + numStar;
    if (numGroup != numTot) {
      throw new DbException(
          "Number of group is different from the number of particles. numGroup: "
              + numGroup
              + " num particles: "
              + numTot);
    }
    lineNumber = 0;
  }

  /**
   * Initialize fileNamesToDataInput and number of particles based on the given type of particles.
   *
   * @param pType The type of the particles.
   * @throws DbException The DbException.
   */
  private void initBasedOnParticleType(final ParticleType pType) throws DbException {
    int numRows;
    Map<String, DataInput> fileNameToDataInput;
    switch (pType) {
      case GAS:
        numRows = numGas;
        fileNameToDataInput = gasAttributeFilesToDataInput;
        break;
      case DARK:
        numRows = numDark;
        fileNameToDataInput = darkAttributeFilesToDataInput;
        break;
      case STAR:
        numRows = numStar;
        fileNameToDataInput = starAttributeFilesToDataInput;
        break;
      default:
        throw new DbException("Invalide pType: " + pType);
    }

    try {
      for (String fileName : fileNameToDataInput.keySet()) {
        DataInput dataInputStream = fileNameToDataInput.get(fileName);
        // Read header of the file. (magic, time, iHighWord, nbodies, ndim, code)
        Preconditions.checkArgument(
            dataInputStream.readInt() == NCHILADA_FORMAT,
            fileName + " is not in NChilada format."); // Read and verify magic.
        // Time.
        dataInputStream.readDouble();
        // IHighWord.
        dataInputStream.readInt();
        // Nbodies.
        int nbodies = dataInputStream.readInt();
        // Ndim;
        int ndim = dataInputStream.readInt();
        if (fileName.equals(POS_FILE_NAME) || fileName.equals(VEL_FILE_NAME)) {
          Preconditions.checkArgument(
              ndim == VEL_POS_DIM,
              fileName + "should have " + VEL_POS_DIM + " instead of " + ndim + ".");
        }
        if (numRows == -1) {
          numRows = nbodies;
          // Update number of particles according to the type.
          switch (pType) {
            case DARK:
              numDark = numRows;
              break;
            case GAS:
              numGas = numRows;
              break;
            case STAR:
              numStar = numRows;
              break;
            default:
              throw new DbException("Invalide pType: " + pType);
          }
        } else {
          Preconditions.checkArgument(
              numRows == nbodies,
              "The files do not have the same number of rows. numRows: "
                  + numRows
                  + " nbodies: "
                  + nbodies
                  + " fileName: "
                  + fileName);
        }
        // Code.
        int code = dataInputStream.readInt();
        Preconditions.checkArgument(
            code == FLOAT_CODE || code == INT_CODE,
            "This code format: " + code + " is not being expected.");

        // After the header, there is the maximum and minimum value in the file, both in the same data type as the rest
        // of the file.
        if (code == FLOAT_CODE) {
          // Max value.
          dataInputStream.readFloat();
          // Min value.
          dataInputStream.readFloat();
        } else {
          // Max value.
          dataInputStream.readInt();
          // Min value.
          dataInputStream.readInt();
        }
      }
    } catch (FileNotFoundException e) {
      throw new DbException(e);
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  /**
   * Constructs tuples of particles. Attributes of star particle: den, pos, pot, vel, iord, mass, OxMassFrac, soft,
   * smoothlength, tform, ESNRate, massform, metals, igasord, FeMassFrac. Attributes of gas particle: HI, HeI, den, pos,
   * pot , vel, HeII, iord, mass, OxMassFrac, soft, OxMassFracdot, gas smoothlength, FeMassFracdot, ESNRate, Metalsdot,
   * GasDensity, metals, temperature, FeMassFrac, coolontime. Attributes of dark particle: den, pos, pot, vel, iord,
   * mass, soft, smoothlength.
   *
   * @param pType The particle type.
   * @throws DbException The DbException.
   */
  private void processFile(final ParticleType pType) throws DbException {
    int numRows;
    Map<String, DataInput> fileNameToDataInput;
    switch (pType) {
      case DARK:
        numRows = numDark;
        fileNameToDataInput = darkAttributeFilesToDataInput;
        break;
      case GAS:
        numRows = numGas;
        fileNameToDataInput = gasAttributeFilesToDataInput;
        break;
      case STAR:
        numRows = numStar;
        fileNameToDataInput = starAttributeFilesToDataInput;
        break;
      default:
        throw new DbException("Invalide pType: " + pType);
    }
    // TODO(leelee): Put 0 for now to replace null values.
    while (numRows > 0 && buffer.numTuples() < TupleBatch.BATCH_SIZE) {
      lineNumber++;
      int column = 0;
      // -2 to exclude grp, and type.
      for (int i = 0; i < NCHILADA_COLUMN_NAMES.size() - 2; i++) {
        String columnNames = NCHILADA_COLUMN_NAMES.get(i);
        DataInput dataInputStream = fileNameToDataInput.get(columnNames);
        Type type = NCHILADA_COLUMN_TYPES.get(i);
        try {
          if (type.equals(Type.FLOAT_TYPE)) {
            if (columnNames.equals(POS_X)
                || columnNames.equals(POS_Y)
                || columnNames.equals(POS_Z)) {
              dataInputStream = fileNameToDataInput.get(POS_FILE_NAME);
              Preconditions.checkArgument(
                  dataInputStream != null, "Cannot find dataInputStream for " + POS_FILE_NAME);
            } else if (columnNames.equals(VEL_X)
                || columnNames.equals(VEL_Y)
                || columnNames.equals(VEL_Z)) {
              dataInputStream = fileNameToDataInput.get(VEL_FILE_NAME);
              Preconditions.checkArgument(
                  dataInputStream != null, "Cannot find dataInputStream for " + VEL_FILE_NAME);
            }
            if (dataInputStream != null) {
              buffer.putFloat(column++, dataInputStream.readFloat());
            } else {
              buffer.putFloat(column++, 0);
            }
          } else {
            if (dataInputStream != null) {
              buffer.putInt(column++, dataInputStream.readInt());
            } else {
              buffer.putInt(column++, 0);
            }
          }
        } catch (IOException e) {
          throw new DbException(e);
        }
      }
      buffer.putInt(column++, groupScanner.nextInt());
      buffer.putString(column++, pType.toString().toLowerCase());
      final String groupRest = groupScanner.nextLine().trim();
      if (groupRest.length() > 0) {
        throw new DbException(
            "groupFile: Unexpected output at the end of line " + lineNumber + ": " + groupRest);
      }
      numRows--;
    }

    // Update number of particles according to the type.
    switch (pType) {
      case DARK:
        numDark = numRows;
        break;
      case GAS:
        numGas = numRows;
        break;
      case STAR:
        numStar = numRows;
        break;
      default:
        throw new DbException("Invalide pType: " + pType);
    }
  }

  @Override
  protected Schema generateSchema() {
    return NCHILADA_SCHEMA;
  }

  @Override
  protected final void cleanup() throws DbException {
    groupScanner = null;
    while (buffer.numTuples() > 0) {
      buffer.popAny();
    }
  }
}
