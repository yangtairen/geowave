package mil.nga.giat.geowave.core.store.index.text;

import junit.framework.Assert;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

import org.junit.Test;

public class TextRangeFilterTest
{
	@Test
	public void testSerialization() {
		final TextRangeFilter filter = new TextRangeFilter(
				new ByteArrayId(
						StringUtils.stringToBinary("myAttribute")),
				false,
				"start",
				"end");
		final byte[] filterBytes = PersistenceUtils.toBinary(filter);
		final TextRangeFilter deserializedFilter = PersistenceUtils.fromBinary(
				filterBytes,
				TextRangeFilter.class);
		Assert.assertTrue(filter.fieldId.equals(deserializedFilter.fieldId));
		Assert.assertTrue(filter.caseSensitive == deserializedFilter.caseSensitive);
		Assert.assertTrue(filter.start.equals(deserializedFilter.start));
		Assert.assertTrue(filter.end.equals(deserializedFilter.end));
	}

	@Test
	public void testAccept() {
		final TextRangeFilter filter = new TextRangeFilter(
				new ByteArrayId(
						"myAttribute"),
				false,
				"dan",
				"derek");

		// should match range
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
										StringUtils.stringToBinary("deke")))));
		Assert.assertTrue(filter.accept(persistenceEncoding));

		// should not match range
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
										StringUtils.stringToBinary("dez")))));
		Assert.assertFalse(filter.accept(persistenceEncoding2));

		// should not match because of fieldId
		final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding3 = new IndexedPersistenceEncoding<ByteArrayId>(
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								new ByteArrayId(
										"mismatch"),
								new ByteArrayId(
										StringUtils.stringToBinary("deke")))));
		Assert.assertFalse(filter.accept(persistenceEncoding3));
	}
}
