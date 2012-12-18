package edu.washington.escience.myriad.operator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.util.Objects;

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
  /** The file this scan is reading from. */
  private final File file;
  /** The Schema of the relation stored in this file. */
  private final Schema schema;
  /** StringTokenizer used to parse the file. */
  private StreamTokenizer tokenizer = null;
  /** Whether a comma is a delimiter in this file. */
  private final boolean commaIsDelimiter;
  /** Holds the tuples that are ready for release. */
  private final TupleBatchBuffer buffer;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line.
   * 
   * @param filename the file containing the relation.
   * @param schema the Schema of the relation contained in the file.
   */
  public FileScan(final String filename, final Schema schema) {
    Objects.requireNonNull(filename);
    Objects.requireNonNull(schema);
    file = new File(filename);
    this.schema = schema;
    commaIsDelimiter = false;
    buffer = new TupleBatchBuffer(schema);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line. If commaIsDelimiter is true, then records may be whitespace or comma-separated.
   * 
   * @param filename the file containing the relation.
   * @param schema the Schema of the relation contained in the file.
   * @param commaIsDelimiter whether commas are also delimiters in the file.
   */
  public FileScan(final String filename, final Schema schema, final boolean commaIsDelimiter) {
    Objects.requireNonNull(filename);
    Objects.requireNonNull(schema);
    file = new File(filename);
    this.schema = schema;
    this.commaIsDelimiter = commaIsDelimiter;
    buffer = new TupleBatchBuffer(schema);
  }

  @Override
  public void init() throws DbException {
    try {
      tokenizer = new StreamTokenizer(new BufferedReader(new FileReader(file)));
    } catch (FileNotFoundException e) {
      throw new DbException(e);
    }
    tokenizer.eolIsSignificant(true);
    if (commaIsDelimiter) {
      tokenizer.whitespaceChars(',', ',');
    }
    try {
      tokenizer.nextToken();
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    int count = 0;
    while (tokenizer.ttype != StreamTokenizer.TT_EOF && buffer.numTuples() < TupleBatch.BATCH_SIZE) {
      /* First, make sure that if we hit EOL we're at the right number of fields full. */
      if (tokenizer.ttype == StreamTokenizer.TT_EOL) {
        if (count == schema.numFields()) {
          count = 0;
          try {
            tokenizer.nextToken();
          } catch (IOException e) {
            throw new DbException(e);
          }
          continue;
        } else {
          throw new DbException("Line " + tokenizer.lineno() + " does not match the given schema");
        }
      }

      /* Second, make sure that if we didn't hit EOL we're at less than the right number of fields. */
      if (count >= schema.numFields()) { // == should work here, but >= to be safe.
        throw new DbException("Line " + tokenizer.lineno() + " does not match the given schema");
      }

      /* Make sure the token is either a word or a number. */
      if (tokenizer.ttype != StreamTokenizer.TT_NUMBER && tokenizer.ttype != StreamTokenizer.TT_WORD) {
        throw new DbException("Error parsing column " + count + " of row " + tokenizer.lineno());
      }

      /* Make sure the schema matches. */
      try {
        switch (schema.getFieldType(count)) {
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
      } catch (NumberFormatException e) {
        throw new DbException("Error parsing column " + count + " of row " + tokenizer.lineno());
      }

      /* Advance to next column. */
      try {
        tokenizer.nextToken();
      } catch (IOException e) {
        throw new DbException(e);
      }
      ++count;
    }
    return buffer.popAny();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void cleanup() {
    tokenizer = null;
    while (buffer.numTuples() > 0) {
      buffer.popAny();
    }
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }
}
