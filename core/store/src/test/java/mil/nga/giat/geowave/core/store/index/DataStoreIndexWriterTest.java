package mil.nga.giat.geowave.core.store.index;

import java.io.Closeable;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.MockEntry;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;

public class DataStoreIndexWriterTest
{
	private static String TEST_NAMESPACE = "geowave.base_store_test";
	private DataStoreIndexWriter<MockEntry, Object> testDataStore;

	@Before
	public void setup() {
		testDataStore = new MockDataStoreIndexWriter(
				new IngestCallback<MockEntry>() {

					@Override
					public void entryIngested(
							final DataStoreEntryInfo entryInfo,
							final MockEntry entry ) {
						// TODO Perhaps assert or have some check that this
						// callback is being called per entry

					}
				},
				new Closeable() {

					@Override
					public void close()
							throws IOException {
						// TODO Perhaps assert close is called appropriately

					}
				},
				TEST_NAMESPACE);
	}

	@Test
	public void testGetIndices() {
		// TODO GEOWAVE-1004, test this DataStoreIndexWriter method
	}

	@Test
	public void testWrite() {
		// TODO GEOWAVE-1004, test this DataStoreIndexWriter method
	}

	@Test
	public void testClose() {
		// TODO GEOWAVE-1004, test this DataStoreIndexWriter method
	}

	@Test
	public void testFlush() {
		// TODO GEOWAVE-1004, test this DataStoreIndexWriter method

	}
}
