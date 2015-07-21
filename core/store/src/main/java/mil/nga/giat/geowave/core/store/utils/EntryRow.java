package mil.nga.giat.geowave.core.store.utils;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;

public class EntryRow implements
		Comparable<EntryRow>
{
	final EntryRowID rowId;
	final DataStoreEntryInfo info;
	final Object entry;

	public EntryRow(
			final ByteArrayId rowId,
			final Object entry,
			final DataStoreEntryInfo info ) {
		super();
		this.rowId = new EntryRowID(
				rowId.getBytes());
		this.entry = entry;
		this.info = info;
	}

	public EntryRowID getTableRowId() {
		return rowId;
	}

	public ByteArrayId getRowId() {
		return new ByteArrayId(
				rowId.getRowId());
	}

	public List<FieldInfo> getColumns() {
		return info.getFieldInfo();
	}

	@Override
	public int compareTo(
			EntryRow o ) {
		return rowId.compareTo(((EntryRow) o).rowId);
	}

	public Object getEntry() {
		return entry;
	}

	public DataStoreEntryInfo getInfo() {
		return info;
	}

}
