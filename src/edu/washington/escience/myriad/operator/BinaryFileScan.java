package edu.washington.escience.myriad.operator;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.InputMismatchException;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.LittleEndianDataInputStream;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;

/**
 * Reads data from binary file. This class is written base on the code from
 * FileScan.java
 * 
 * @author leelee
 * 
 */

public class BinaryFileScan extends LeafOperator {

	/** Required for Java serialization */
	private static final long serialVersionUID = 1L;
	/** The schema for the relation stored in this file */
	private final Schema schema;
	/** The filename of the input */
	private final String fileName;
	/** Holds the tuples that are ready for release */
	private transient TupleBatchBuffer buffer;
	/** Which line of the file the scanner is currently on */
	private int lineNumber;
	/** Keep tracks of the length left to read */
	private long lenLeft;
	private final boolean isLittleEndian;
	private FileInputStream fStream;
	private DataInput dataInput;

	public FileOutputStream output;

	// add little endian boolean, default false
	// use DataInput interface, littleendianinputstream else new datainputstream
	// start with ten records first

	public BinaryFileScan(Schema schema, String fileName, boolean isLittleEndian) {
		Objects.requireNonNull(schema);
		Objects.requireNonNull(fileName);
		this.schema = schema;
		this.fileName = fileName;
		try {
			output = new FileOutputStream(new File("cosmo24star.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.isLittleEndian = isLittleEndian;
	}

	public BinaryFileScan(Schema schema, String fileName) {
		this(schema, fileName, false);
	}

	@Override
	protected TupleBatch fetchNext() throws DbException {
		System.out.println("in fecth next");
		try {
			while (fStream.available() > 0
			    && buffer.numTuples() < TupleBatch.BATCH_SIZE) {
				lineNumber++;
				for (int count = 0; count < schema.numColumns(); ++count) {
					/* Make sure the schema matches */
					try {
						switch (schema.getColumnType(count)) {
						case BOOLEAN_TYPE:
							buffer.put(count, dataInput.readBoolean());
							lenLeft -= 1;
							break;
						case DOUBLE_TYPE:
							buffer.put(count, dataInput.readDouble());
							lenLeft -= 8;
							break;
						case FLOAT_TYPE:
							float readFloat = dataInput.readFloat();
							buffer.put(count, readFloat);
							lenLeft -= 4;
							break;
						case INT_TYPE:
							int readInt = dataInput.readInt();
							buffer.put(count, readInt);
							lenLeft -= 4;
							break;
						case LONG_TYPE:
							long readLong = dataInput.readLong();
							buffer.put(count, readLong);
							lenLeft -= 8;
							break;
						case STRING_TYPE:
							// buffer.put(count, raf.read);
							break;
						default:
							break;
						}
					} catch (final InputMismatchException e) {
						throw new DbException("Error parsing column " + count + " of row "
						    + lineNumber + ": " + e.toString());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new DbException(e.getMessage());
		}
		TupleBatch tb = buffer.popAny();
		try {
			if (tb != null) {
				output.write(tb.toString().getBytes());
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tb;
	}

	@Override
	protected void cleanup() throws DbException {
		while (buffer.numTuples() > 0) {
			buffer.popAny();
		}
	}

	@Override
	protected TupleBatch fetchNextReady() throws DbException {
		return fetchNext();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	protected void init(ImmutableMap<String, Object> execEnvVars)
	    throws DbException {
		buffer = new TupleBatchBuffer(getSchema());
		// try {
		// raf = new RandomAccessFile(fileName, "r");
		// lenLeft = raf.length();
		// } catch (FileNotFoundException e) {
		// e.printStackTrace();
		// return;
		// } catch (IOException e) {
		// e.printStackTrace();
		// return;
		// }
		System.out.println("in init");
		if (fileName != null) {
			fStream = null;
			try {
				fStream = new FileInputStream(fileName);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				System.out.println("problem with creating fileInputStream");
				e.printStackTrace();
			}
			if (isLittleEndian) {
				dataInput = new LittleEndianDataInputStream(fStream);
			} else {
				System.out.println("big endian");
				dataInput = new DataInputStream(fStream);
			}
			// lenLeft = fStream.
		}
		lineNumber = 0;
	}
}
