package edu.washington.escience.myria.column.mutable;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.MyriaMatrix;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.MyriaMatrixColumn;

/**
 * A mutable column of Date values.
 * 
 */
public final class MyriaMatrixMutableColumn extends MutableColumn<MyriaMatrix> {
	/** Required for Java serialization. */
	private static final long serialVersionUID = 1L;
	/** Internal representation of the column data. */
	private final MyriaMatrix[] data;
	/** The number of existing rows in this column. */
	private final int position;

	/**
	 * Constructs a new column.
	 * 
	 * @param data
	 *            the data
	 * @param numData
	 *            number of tuples.
	 * */
	public MyriaMatrixMutableColumn(final MyriaMatrix[] data, final int numData) {
		this.data = data;
		position = numData;
	}

	@Override
	@Deprecated
	public MyriaMatrix getObject(final int row) {
		return getMyriaMatrix(row);
	}

	@Override
	public MyriaMatrix getMyriaMatrix(final int row) {
		Preconditions.checkElementIndex(row, position);
		return data[row];
	}

	@Override
	public Type getType() {
		return Type.DATETIME_TYPE;
	}

	@Override
	public int size() {
		return position;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(size()).append(" elements: [");
		for (int i = 0; i < size(); ++i) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append(data[i]);
		}
		sb.append(']');
		return sb.toString();
	}

	@Override
	public void replaceMyriaMatrix(final MyriaMatrix value, final int row) {
		Preconditions.checkElementIndex(row, size());
		data[row] = value;
	}

	@Override
	public MyriaMatrixColumn toColumn() {
		return new MyriaMatrixColumn(data.clone(), position);
	}

	@Override
	public MyriaMatrixMutableColumn clone() {
		return new MyriaMatrixMutableColumn(data.clone(), position);
	}
}