package mil.nga.giat.geowave.datastore.hbase;

import org.apache.hadoop.hbase.client.Result;

import mil.nga.giat.geowave.core.store.entities.GeowaveRowId;
import mil.nga.giat.geowave.core.store.entities.NativeGeoWaveRow;

public class HBaseRow implements
		NativeGeoWaveRow
{
	private final byte[] dataId;
	private final byte[] adapterId;
	private final byte[] idx;
	private final byte[] fieldMask;
	private final byte[] value;

	public HBaseRow(
			final byte[] dataId,
			final byte[] adapterId,
			final byte[] idx,
			final byte[] fieldMask,
			final byte[] value ) {
		this.dataId = dataId;
		this.adapterId = adapterId;
		this.idx = idx;
		this.fieldMask = fieldMask;
		this.value = value;
	}
	
	public HBaseRow(Result row) {
		final GeowaveRowId rowId = new GeowaveRowId(
				row.getRow());

		this.dataId = rowId.getDataId();
		this.adapterId = rowId.getAdapterId();
		this.idx = null; // TODO
		this.fieldMask = null; // TODO
		this.value = null; // TODO
	}

	@Override
	public byte[] getDataId() {
		return dataId;
	}

	@Override
	public byte[] getAdapterId() {
		return adapterId;
	}

	@Override
	public byte[] getFieldMask() {
		return fieldMask;
	}

	@Override
	public byte[] getValue() {
		return value;
	}

	@Override
	public byte[] getIndex() {
		return idx;
	}

}
