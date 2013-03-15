package edu.washington.escience.myriad.operator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StreamTokenizer;
import java.util.Objects;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;

/**
 * Reads data from a file.
 * 
 * @author dhalperi
 * 
 */
public final class FileScan extends LeafOperator {
  /** The input stream that this scan is reading from. */
  private transient InputStream inputStream = null;
  /** The Schema of the relation stored in this file. */
  private final Schema schema;
  /** StringTokenizer used to parse the file. */
  private transient StreamTokenizer tokenizer = null;
  /** Whether a comma is a delimiter in this file. */
  private final boolean commaIsDelimiter;
  private String filename = null;
  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line.
   * 
   * @param filename the file containing the relation.
   * @param schema the Schema of the relation contained in the file.
   * @throws FileNotFoundException if the given filename does not exist.
   */
  public FileScan(final String filename, final Schema schema) throws FileNotFoundException {
    this(filename, schema, false);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line. If commaIsDelimiter is true, then records may be whitespace or comma-separated.
   * 
   * @param filename the file containing the relation.
   * @param schema the Schema of the relation contained in the file.
   * @param commaIsDelimiter whether commas are also delimiters in the file.
   * @throws FileNotFoundException if the given filename does not exist.
   */
  public FileScan(final String filename, final Schema schema, final boolean commaIsDelimiter)
      throws FileNotFoundException {
    Objects.requireNonNull(filename);
    Objects.requireNonNull(schema);
    this.schema = schema;
    this.commaIsDelimiter = commaIsDelimiter;
    this.filename = filename;
  }

  /**
   * Construct a new FileScan object to read from a inputStream. If commaIsDelimiter is true, then records may be
   * whitespace or comma-separated. inputStream is assumed to be set later by setInputStream().
   * 
   * @param schema the Schema of the relation contained in the file.
   * @param commaIsDelimiter whether commas are also delimiters in the file.
   */
  public FileScan(final Schema schema, final boolean commaIsDelimiter) {
    this.schema = schema;
    this.commaIsDelimiter = commaIsDelimiter;
  }

  /**
   * Construct a new FileScan object to read from a input stream. The data is assumed to be whitespace-separated and
   * have one record per line. inputStream is assumed to be set later by setInputStream().
   * 
   * @param schema the Schema of the relation contained in the file.
   */
  public FileScan(final Schema schema) {
    this(schema, false);
  }

  /**
   * @param inputStream the data containing the relation.
   */
  public void setInputStream(final InputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public void cleanup() {
    tokenizer = null;
    while (buffer.numTuples() > 0) {
      buffer.popAny();
    }
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    int count = 0;
    boolean flag = true;
    while (tokenizer.ttype != StreamTokenizer.TT_EOF && (buffer.numTuples() < TupleBatch.BATCH_SIZE || flag)) {
      /* First, make sure that if we hit EOL we're at the right number of fields full. */
      if (tokenizer.ttype == StreamTokenizer.TT_EOL) {
        if (count == schema.numColumns()) {
          count = 0;
          try {
            tokenizer.nextToken();
          } catch (final IOException e) {
            throw new DbException(e);
          }
          flag = false;
          continue;
        } else {
          throw new DbException("Line " + tokenizer.lineno() + " does not match the given schema");
        }
      }
      flag = true;

      /* Second, make sure that if we didn't hit EOL we're at less than the right number of fields. */
      if (count >= schema.numColumns()) { // == should work here, but >= to be safe.
        throw new DbException("Line " + tokenizer.lineno() + " does not match the given schema");
      }

      /* Make sure the token is either a word or a number. */
      if (tokenizer.ttype != StreamTokenizer.TT_NUMBER && tokenizer.ttype != StreamTokenizer.TT_WORD) {
        throw new DbException("Error parsing column " + count + " of row " + tokenizer.lineno());
      }

      /* Make sure the schema matches. */
      try {
        switch (schema.getColumnType(count)) {
          case BOOLEAN_TYPE:
            if (tokenizer.ttype == StreamTokenizer.TT_NUMBER) {
              buffer.put(count, Boolean.valueOf(tokenizer.nval != 0));
            } else {
              buffer.put(count, Boolean.parseBoolean(tokenizer.sval));
            }
            break;
          case DOUBLE_TYPE:
            if (tokenizer.ttype == StreamTokenizer.TT_NUMBER) {
              buffer.put(count, tokenizer.nval);
            } else {
              buffer.put(count, Double.parseDouble(tokenizer.sval));
            }
            break;
          case FLOAT_TYPE:
            if (tokenizer.ttype == StreamTokenizer.TT_NUMBER) {
              buffer.put(count, tokenizer.nval);
            } else {
              buffer.put(count, Float.parseFloat(tokenizer.sval));
            }
            break;
          case INT_TYPE:
            if (tokenizer.ttype == StreamTokenizer.TT_NUMBER) {
              if (tokenizer.nval != (int) tokenizer.nval) {
                throw new DbException("Error parsing column " + count + " of row " + tokenizer.lineno());
              }
              buffer.put(count, (int) tokenizer.nval);
            } else {
              buffer.put(count, Integer.parseInt(tokenizer.sval));
            }
            break;
          case LONG_TYPE:
            if (tokenizer.ttype == StreamTokenizer.TT_NUMBER) {
              if (tokenizer.nval != (long) tokenizer.nval) {
                throw new DbException("Error parsing column " + count + " of row " + tokenizer.lineno());
              }
              buffer.put(count, (long) tokenizer.nval);
            } else {
              buffer.put(count, Long.parseLong(tokenizer.sval));
            }
            break;
          case STRING_TYPE:
            buffer.put(count, tokenizer.sval);
            break;
        }
      } catch (final NumberFormatException e) {
        throw new DbException("Error parsing column " + count + " of row " + tokenizer.lineno());
      }

      /* Advance to next column. */
      try {
        tokenizer.nextToken();
      } catch (final IOException e) {
        throw new DbException(e);
      }
      ++count;
    }
    return buffer.popAny();
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void init() throws DbException {
    buffer = new TupleBatchBuffer(getSchema());
    if (filename != null) {
      try {
        inputStream = new FileInputStream(filename);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
        return;
      }
    }
    Preconditions.checkArgument(inputStream != null, "FileScan input stream has not been set!");
    tokenizer = new StreamTokenizer(new BufferedReader(new InputStreamReader(inputStream)));
    tokenizer.eolIsSignificant(true);
    if (commaIsDelimiter) {
      tokenizer.whitespaceChars(',', ',');
    }
    try {
      tokenizer.nextToken();
    } catch (final IOException e) {
      throw new DbException(e);
    }
  }
}
