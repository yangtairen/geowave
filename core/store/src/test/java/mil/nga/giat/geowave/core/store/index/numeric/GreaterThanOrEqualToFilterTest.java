package mil.nga.giat.geowave.core.store.index.numeric;

import junit.framework.Assert;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

import org.junit.Test;

public class GreaterThanOrEqualToFilterTest
{
	@Test
	public void testSerialization() {
		final GreaterThanOrEqualToFilter filter = new GreaterThanOrEqualToFilter(
				new ByteArrayId(
						StringUtils.stringToBinary("myAttribute")),
				new Double(
						1d));
		final byte[] filterBytes = PersistenceUtils.toBinary(filter);
		final GreaterThanOrEqualToFilter deserializedFilter = PersistenceUtils.fromBinary(
				filterBytes,
				GreaterThanOrEqualToFilter.class);
		Assert.assertTrue(filter.fieldId.equals(deserializedFilter.fieldId));
		Assert.assertTrue(filter.number.equals(deserializedFilter.number));
	}

	@Test
	public void testAccept() {
		final GreaterThanOrEqualToFilter filter = new GreaterThanOrEqualToFilter(
				new ByteArrayId(
						"myAttribute"),
				new Double(
						10d));

		// should pass because 100 >= 10
		final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding = new IndexedPersistenceEncoding<ByteArrayId>(
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								new ByteArrayId(
										"myAttribute"),
								new ByteArrayId(
										Lexicoders.DOUBLE.toByteArray(100d)))));
		Assert.assertTrue(filter.accept(persistenceEncoding));

		// should pass because 10 >= 10
		final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding2 = new IndexedPersistenceEncoding<ByteArrayId>(
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								new ByteArrayId(
										"myAttribute"),
								new ByteArrayId(
										Lexicoders.DOUBLE.toByteArray(10d)))));
		Assert.assertTrue(filter.accept(persistenceEncoding2));

		// should not pass on 9 >= 10
		final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding3 = new IndexedPersistenceEncoding<ByteArrayId>(
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								new ByteArrayId(
										"myAttribute"),
								new ByteArrayId(
										Lexicoders.DOUBLE.toByteArray(9d)))));
		Assert.assertFalse(filter.accept(persistenceEncoding3));

		// should not pass because of fieldId mismatch
		final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding4 = new IndexedPersistenceEncoding<ByteArrayId>(
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								new ByteArrayId(
										"mismatch"),
								new ByteArrayId(
										Lexicoders.DOUBLE.toByteArray(100d)))));
		Assert.assertFalse(filter.accept(persistenceEncoding4));
	}
}
