package mil.nga.giat.geowave.core.store.base;

import org.junit.Before;
import org.junit.Test;

public class BaseDataStoreTest
{
	private static String TEST_NAMESPACE = "geowave.base_store_test";
	private BaseDataStore testDataStore;

	@Before
	public void setup() {
		testDataStore = new MockBaseDataStore(
				TEST_NAMESPACE);
	}

	@Test
	public void testStoreIndex() {
		// TODO GEOWAVE-1004, test this BaseDataStore method
	}

	@Test
	public void testStoreAdapter() {
		// TODO GEOWAVE-1004, test this BaseDataStore method
	}

	@Test
	public void testCreateWriter() {
		// TODO GEOWAVE-1004, test this BaseDataStore method
	}

	@Test
	public void testQuery() {
		// TODO GEOWAVE-1002, test this BaseDataStore method
	}

	@Test
	public void testGetEntries() {
		// TODO GEOWAVE-1002, test this BaseDataStore method
	}

	@Test
	public void testDelete() {
		// TODO GEOWAVE-1003, test this BaseDataStore method
	}

	@Test
	public void testDeleteEntries() {
		// TODO GEOWAVE-1003, test this BaseDataStore method
	}

}
