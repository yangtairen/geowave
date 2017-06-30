package mil.nga.giat.geowave.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.mapreduce.GeoWaveKey;

import org.apache.hadoop.io.WritableComparator;

/**
 * This class encapsulates the unique identifier for GeoWave input data using a
 * map-reduce GeoWave input format. The combination of the the adapter ID and
 * the data ID should be unique.
 */
public class GeoWaveInputKey extends
		GeoWaveKey
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private ByteArrayId dataId;
	private transient ByteArrayId partitionKey;
	private transient ByteArrayId sortKey;

	public GeoWaveInputKey() {
		super();
	}

	public GeoWaveInputKey(
			final ByteArrayId adapterId,
			final ByteArrayId dataId ) {
		super(
				adapterId);
		this.dataId = dataId;
	}

	public ByteArrayId getPartitionKey() {
		return partitionKey;
	}

	public void setPartitionKey(
			ByteArrayId partitionKey ) {
		this.partitionKey = partitionKey;
	}

	public ByteArrayId getSortKey() {
		return sortKey;
	}

	public void setSortKey(
			ByteArrayId sortKey ) {
		this.sortKey = sortKey;
	}

	public void setDataId(
			final ByteArrayId dataId ) {
		this.dataId = dataId;
	}

	public ByteArrayId getDataId() {
		return dataId;
	}

	@Override
	public int compareTo(
			final GeoWaveKey o ) {
		final int baseCompare = super.compareTo(o);
		if (baseCompare != 0) {
			return baseCompare;
		}
		if (o instanceof GeoWaveInputKey) {
			final GeoWaveInputKey other = (GeoWaveInputKey) o;
			final int dataIdCompare = WritableComparator.compareBytes(
					dataId.getBytes(),
					0,
					dataId.getBytes().length,
					other.dataId.getBytes(),
					0,
					other.dataId.getBytes().length);
			if (dataIdCompare != 0) {
				return dataIdCompare;
			}
			if (partitionKey != null) {
				int partitionKeyCompare = partitionKey.compareTo(other.partitionKey);

				if (partitionKeyCompare != 0) {
					return partitionKeyCompare;
				}
			}
			else if (other.partitionKey != null) {
				return 1;
			}
			if (sortKey != null) {
				int sortKeyCompare = sortKey.compareTo(other.sortKey);

				if (sortKeyCompare != 0) {
					return sortKeyCompare;
				}
			}
			else if (other.sortKey != null) {
				return 1;
			}
		}
		return 1;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = (prime * result) + ((dataId == null) ? 0 : dataId.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final GeoWaveInputKey other = (GeoWaveInputKey) obj;
		if (dataId == null) {
			if (other.dataId != null) {
				return false;
			}
		}
		else if (!dataId.equals(other.dataId)) {
			return false;
		}
		return true;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		super.readFields(input);
		final int dataIdLength = input.readInt();
		final byte[] dataIdBytes = new byte[dataIdLength];
		input.readFully(dataIdBytes);
		dataId = new ByteArrayId(
				dataIdBytes);
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		super.write(output);
		output.writeInt(dataId.getBytes().length);
		output.write(dataId.getBytes());
	}
}
