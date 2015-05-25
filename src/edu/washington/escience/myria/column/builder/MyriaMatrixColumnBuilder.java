package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.nio.LongBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.MyriaMatrix;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.MyriaMatrixColumn;
import edu.washington.escience.myria.column.mutable.MyriaMatrixMutableColumn;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.MyriaMatrixColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A column of Date values.
 * 
 */
public final class MyriaMatrixColumnBuilder extends ColumnBuilder<MyriaMatrix> {

	/**
	 * The internal representation of the data.
	 * */
	private final MyriaMatrix[] data;

	/** Number of elements in this column. */
	private int numMatrices;

	/**
	 * If the builder has built the column.
	 * */
	private boolean built = false;

	/**
	 * Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE
	 * elements.
	 */
	public MyriaMatrixColumnBuilder() {
		numMatrices = 0;
		data = new MyriaMatrix[TupleBatch.BATCH_SIZE];
	}

	/**
	 * copy.
	 * 
	 * @param numMatrices
	 *            the actual num strings in the data
	 * @param data
	 *            the underlying data
	 * */
	private MyriaMatrixColumnBuilder(final MyriaMatrix[] data,
			final int numMatrices) {
		this.numMatrices = numMatrices;
		this.data = data;
	}

	/**
	 * Constructs a MatrixColumn by deserializing the given ColumnMessage.
	 * 
	 * @param message
	 *            a ColumnMessage containing the contents of this column.
	 * @param numTuples
	 *            num tuples in the column message
	 * @return the built column
	 */
	public static MyriaMatrixColumn buildFromProtobuf(
			final ColumnMessage message, final int numTuples) {
		Preconditions
				.checkArgument(
						message.getType().ordinal() == ColumnMessage.Type.MYRIAMATRIX_VALUE,
						"Trying to construct MatrixColumn from non-DATE ColumnMessage %s",
						message.getType());
		Preconditions.checkArgument(message.hasMatrixColumn(),
				"ColumnMessage is missing MatrixColumn");
		final MyriaMatrixColumnMessage matrixColumn = message.getMatrixColumn();
		MyriaMatrix[] newData = new MyriaMatrix[numTuples];
		LongBuffer data = matrixColumn.getData().asReadOnlyByteBuffer()
				.asLongBuffer();
		for (int i = 0; i < numTuples; i++) {
			newData[i] = new MyriaMatrix(data.get());
		}
		return new MyriaMatrixColumnBuilder(newData, numTuples).build();
	}

	@Override
	public MyriaMatrixColumnBuilder appendMyriaMatrix(final MyriaMatrix value)
			throws BufferOverflowException {
		Preconditions
				.checkState(!built,
						"No further changes are allowed after the builder has built the column.");
		Objects.requireNonNull(value, "value");
		if (numMatrices >= TupleBatch.BATCH_SIZE) {
			throw new BufferOverflowException();
		}
		data[numMatrices++] = value;
		return this;
	}

	@Override
	public Type getType() {
		return Type.MYRIAMATRIX_TYPE;
	}

	/**
	 * Not yet supported. This is based on datetime.
	 */
	@Override
	public MyriaMatrixColumnBuilder appendFromJdbc(final ResultSet resultSet,
			final int jdbcIndex) throws SQLException, BufferOverflowException {
		Preconditions
				.checkState(!built,
						"No further changes are allowed after the builder has built the column.");
		return appendMyriaMatrix(new MyriaMatrix(resultSet.getTimestamp(
				jdbcIndex).getTime()));
	}

	/**
	 * Not yet supported. This is based on datetime.
	 */
	@Override
	public MyriaMatrixColumnBuilder appendFromSQLite(
			final SQLiteStatement statement, final int index)
			throws SQLiteException, BufferOverflowException {
		Preconditions
				.checkState(!built,
						"No further changes are allowed after the builder has built the column.");

		return appendMyriaMatrix(new MyriaMatrix(statement.columnLong(index)));
	}

	@Override
	public int size() {
		return numMatrices;
	}

	@Override
	public MyriaMatrixColumn build() {
		built = true;
		return new MyriaMatrixColumn(data, numMatrices);
	}

	@Override
	public MyriaMatrixMutableColumn buildMutable() {
		built = true;
		return new MyriaMatrixMutableColumn(data, numMatrices);
	}

	@Override
	public void replaceMyriaMatrix(final MyriaMatrix value, final int row)
			throws IndexOutOfBoundsException {
		Preconditions
				.checkState(!built,
						"No further changes are allowed after the builder has built the column.");
		Preconditions.checkElementIndex(row, numMatrices);
		Preconditions.checkNotNull(value);
		data[row] = value;
	}

	@Override
	public MyriaMatrixColumnBuilder expand(final int size)
			throws BufferOverflowException {
		Preconditions
				.checkState(!built,
						"No further changes are allowed after the builder has built the column.");
		Preconditions.checkArgument(size >= 0);
		if (numMatrices + size > data.length) {
			throw new BufferOverflowException();
		}
		numMatrices += size;
		return this;
	}

	@Override
	public MyriaMatrixColumnBuilder expandAll() {
		Preconditions
				.checkState(!built,
						"No further changes are allowed after the builder has built the column.");
		numMatrices = data.length;
		return this;
	}

	@Override
	public MyriaMatrix getMyriaMatrix(final int row) {
		Preconditions.checkElementIndex(row, numMatrices);
		return data[row];
	}

	@Override
	public MyriaMatrix getObject(final int row) {
		return getMyriaMatrix(row);
	}

	@Deprecated
	@Override
	public MyriaMatrixColumnBuilder appendObject(final Object value)
			throws BufferOverflowException {
		Preconditions
				.checkState(!built,
						"No further changes are allowed after the builder has built the column.");
		return appendMyriaMatrix((MyriaMatrix) MyriaUtils
				.ensureObjectIsValidType(value));
	}

	@Override
	public MyriaMatrixColumnBuilder forkNewBuilder() {
		MyriaMatrix[] newData = new MyriaMatrix[data.length];
		System.arraycopy(data, 0, newData, 0, numMatrices);
		return new MyriaMatrixColumnBuilder(newData, numMatrices);
	}

}