package mil.nga.giat.geowave.core.store.memory;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;

public class MemorySecondaryIndexDataStore implements
		SecondaryIndexDataStore
{

	@Override
	public void storeJoinEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId primaryIndexId,
			ByteArrayId primaryIndexPartitionKey,			
			ByteArrayId primaryIndexSortKey,
			ByteArrayId attributeVisibility ) {
		// TODO Auto-generated method stub

	}


	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeAll() {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> CloseableIterator<T> query(
			final SecondaryIndex<T> secondaryIndex,
			final ByteArrayId indexedAttributeFieldId,
			final DataAdapter<T> adapter,
			final PrimaryIndex primaryIndex,
			final DistributableQuery query,
			final String... authorizations ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setDataStore(
			final DataStore dataStore ) {
		// TODO Auto-generated method stub

	}


	@Override
	public void storeEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId dataId,
			GeoWaveValue... originalFields ) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void deleteJoinEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId primaryIndexPartitionKey,
			ByteArrayId primaryIndexSortKey,
			ByteArrayId attributeVisibility ) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void deleteEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId dataId,
			GeoWaveValue... originalFields ) {
		// TODO Auto-generated method stub
		
	}

}