package mil.nga.giat.geowave.core.store.index;

import java.io.Closeable;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.MockComponents;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.MockDataAdapter;
import mil.nga.giat.geowave.core.store.base.MockDataStoreOperations;
import mil.nga.giat.geowave.core.store.base.MockEntry;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;

public class MockDataStoreIndexWriter extends
		DataStoreIndexWriter<MockEntry, Object>
{

	public MockDataStoreIndexWriter(
			final IngestCallback<MockEntry> callback,
			final Closeable closable,
			String tableNamespace ) {
		super(
				new MockDataAdapter(),
				new PrimaryIndex(
						new MockComponents.MockIndexStrategy(),
						new MockComponents.TestIndexModel()),
				new MockDataStoreOperations(
						tableNamespace),
				new BaseDataStoreOptions(),
				callback,
				closable);
	}

	@Override
	protected void ensureOpen() {

		// TODO GEOWAVE-1004, complete this method as necessary for tests
	}

	@Override
	protected DataStoreEntryInfo getEntryInfo(
			final MockEntry entry,
			final VisibilityWriter<MockEntry> visibilityWriter ) {
		// TODO GEOWAVE-1004, complete this method as necessary for tests
		return null;
	}

}
