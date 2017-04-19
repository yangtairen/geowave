package mil.nga.giat.geowave.core.store.adapter.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.FixedBinNumericHistogram.FixedBinNumericHistogramFactory;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;

public class RowHistogramDataStaticticsTest
{

	Random r = new Random(
			347);

	private GeoWaveKey genKey(
			final long bottom,
			final long top ) {
		final InsertionIds insertionIds = new InsertionIds(
				Arrays.asList(
						new ByteArrayId(
								String.format(
										"\12%6h",
										bottom + (r.nextDouble() * (top - bottom))) + "20030f89")));
		return GeoWaveKeyImpl.createKeys(
				insertionIds,
				new byte[] {},
				new byte[] {})[0];
	}

	RowRangeHistogramStatistics<Integer> stats1 = new RowRangeHistogramStatistics<Integer>(
			new ByteArrayId(
					"20030"),
			new ByteArrayId(
					"20030"),
			new FixedBinNumericHistogramFactory(),
			1024);

	RowRangeHistogramStatistics<Integer> stats2 = new RowRangeHistogramStatistics<Integer>(
			new ByteArrayId(
					"20030"),
			new ByteArrayId(
					"20030"));

	@Test
	public void testId() {
		assertEquals(
				stats1.getStatisticsId(),
				stats1.duplicate().getStatisticsId());

	}

	@Test
	public void testIngest() {
		for (long i = 0; i < 10000; i++) {
			final GeoWaveRow row = new GeoWaveRowImpl(
							genKey(
									0,
									100000),
					new GeoWaveValue[] {});
			stats1.entryIngested(
					1,
					row);
			stats2.entryIngested(
					1,
					row);
		}

		for (int i = 1000; i < 100000; i += 1000) {
			final byte[] half = genKey(
					i,
					i + 1).getSortKey();
			final double diff = Math.abs(
					stats1.cdf(
							half)
							- stats2.cdf(
									half));
			assertTrue(
					"iteration " + i + " = " + diff,
					diff < 0.02);
		}

		System.out.println(
				"-------------------------");

		for (long j = 10000; j < 20000; j++) {
			final GeoWaveRow row = new GeoWaveRowImpl(
					genKey(
									100000,
									200000),
					new GeoWaveValue[] {});
			stats1.entryIngested(
					1,
					row);
			stats2.entryIngested(
					1,
					row);
		}

		for (int i = 1000; i < 100000; i += 1000) {
			final byte[] half = genKey(
					i,
					i + 1).getSortKey();
			final double diff = Math.abs(
					stats1.cdf(
							half)
							- stats2.cdf(
									half));
			assertTrue(
					"iteration " + i + " = " + diff,
					diff < 0.02);
		}

		final byte[] nearfull = genKey(
				79998,
				89999).getSortKey();
		double diff = Math.abs(
				stats1.cdf(
						nearfull)
						- stats2.cdf(
								nearfull));
		assertTrue(
				"nearfull = " + diff,
				diff < 0.02);
		final byte[] nearempty = genKey(
				9998,
				9999).getSortKey();
		diff = Math.abs(
				stats1.cdf(
						nearempty)
						- stats2.cdf(
								nearempty));
		assertTrue(
				"nearempty = " + diff,
				diff < 0.02);
	}
}
