package mil.nga.giat.geowave.core.store.adapter.statistics;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;

public class RowRangeDataStaticticsTest
{

	@Test
	public void testEmpty() {
		final RowRangeDataStatistics<Integer> stats = new RowRangeDataStatistics<Integer>(
				new ByteArrayId(
						"20030"));

		assertFalse(
				stats.isSet());

		stats.fromBinary(
				stats.toBinary());
	}

	private GeoWaveRow[] genRows(
			final List<ByteArrayId> sortKeys,
			final byte[] dataId ) {
		final InsertionIds insertionIds = new InsertionIds(
				sortKeys);
		return Lists.transform(
				Arrays.asList(
						GeoWaveKeyImpl.createKeys(
								insertionIds,
								dataId,
								new byte[] {})),
				new Function<GeoWaveKey, GeoWaveRow>() {

					@Override
					public GeoWaveRow apply(
							final GeoWaveKey input ) {
						return new GeoWaveRowImpl(
								input,
								new GeoWaveValue[] {});
					}

				}).toArray(
						new GeoWaveRow[] {});
	}

	@Test
	public void testIngest() {
		final RowRangeDataStatistics<Integer> stats = new RowRangeDataStatistics<Integer>(
				new ByteArrayId(
						"20030"));

		List<ByteArrayId> sortKeys = Arrays.asList(
				new ByteArrayId(
						"20030"),
				new ByteArrayId(
						"014"),
				new ByteArrayId(
						"0124"),
				new ByteArrayId(
						"0123"),
				new ByteArrayId(
						"5064"),
				new ByteArrayId(
						"50632"));

		byte[] dataId = "23".getBytes();
		stats.entryIngested(
				1,
				genRows(
						sortKeys,
						dataId));

		assertTrue(
				Arrays.equals(
						new ByteArrayId(
								"0123").getBytes(),
						stats.getMin()));

		assertTrue(
				Arrays.equals(
						new ByteArrayId(
								"5064").getBytes(),
						stats.getMax()));

		assertTrue(
				stats.isSet());

		// merge

		final RowRangeDataStatistics<Integer> stats2 = new RowRangeDataStatistics<Integer>(
				new ByteArrayId(
						"20030"));
		sortKeys = Arrays.asList(
				new ByteArrayId(
						"20030"),
				new ByteArrayId(
						"014"),
				new ByteArrayId(
						"8062"));
		dataId = "32".getBytes();
		stats2.entryIngested(
				1,
				genRows(
						sortKeys,
						dataId));

		stats.merge(
				stats2);

		assertTrue(
				Arrays.equals(
						new ByteArrayId(
								"0123").getBytes(),
						stats.getMin()));

		assertTrue(
				Arrays.equals(
						new ByteArrayId(
								"8062").getBytes(),
						stats.getMax()));

		stats2.fromBinary(
				stats.toBinary());

		assertTrue(
				Arrays.equals(
						new ByteArrayId(
								"0123").getBytes(),
						stats2.getMin()));

		assertTrue(
				Arrays.equals(
						new ByteArrayId(
								"8062").getBytes(),
						stats2.getMax()));

		stats.toString();
	}
}
