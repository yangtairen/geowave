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

public class EqualsFilterTest
{
	@Test
	public void testSerialization() {
		final EqualsFilter filter = new EqualsFilter(
				new ByteArrayId(
						StringUtils.stringToBinary("myAttribute")),
				new Double(
						1d));
		final byte[] filterBytes = PersistenceUtils.toBinary(filter);
		final EqualsFilter deserializedFilter = PersistenceUtils.fromBinary(
				filterBytes,
				EqualsFilter.class);
		Assert.assertTrue(filter.fieldId.equals(deserializedFilter.fieldId));
		Assert.assertTrue(filter.number.equals(deserializedFilter.number));
	}

	@Test
	public void testAccept() {
		final EqualsFilter filter = new EqualsFilter(
				new ByteArrayId(
						"myAttribute"),
				new Double(
						10d));

		// should pass because 10 == 10
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
										Lexicoders.DOUBLE.toByteArray(10d)))));
		Assert.assertTrue(filter.accept(persistenceEncoding));

		// should not pass on 11 == 10
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
										Lexicoders.DOUBLE.toByteArray(11d)))));
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
										Lexicoders.DOUBLE.toByteArray(10d)))));
		Assert.assertFalse(filter.accept(persistenceEncoding4));
	}
}
