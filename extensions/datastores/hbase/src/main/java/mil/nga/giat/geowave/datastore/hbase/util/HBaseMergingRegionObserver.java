package mil.nga.giat.geowave.datastore.hbase.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseDistributableFilter;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseMergingFilter;

public class HBaseMergingRegionObserver extends
		BaseRegionObserver
{
	private final static Logger LOGGER = Logger.getLogger(
			HBaseMergingRegionObserver.class);

	// TEST ONLY!
	static {
		LOGGER.setLevel(
				Level.DEBUG);
	}

	private HashMap<RegionScanner, Scan> scanMap = new HashMap<RegionScanner, Scan>();

	@Override
	public RegionScanner postScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan,
			final RegionScanner s )
			throws IOException {
		scanMap.put(
				s,
				scan);

		return s;
	}

	@Override
	public boolean postScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s,
			final List<Result> results,
			final int limit,
			final boolean hasMore )
			throws IOException {
		if (results.size() > 0) {
			Scan scan = scanMap.get(
					s);

			String mergeData = null;

			if (scan != null) {
				Filter scanFilter = scan.getFilter();
				if (scanFilter != null) {
					if (scanFilter instanceof FilterList) {
						for (Filter filter : ((FilterList) scanFilter).getFilters()) {
							if (filter instanceof HBaseMergingFilter) {
								HBaseMergingFilter mergingFilter = (HBaseMergingFilter) filter;
								mergeData = mergingFilter.getMergeData();
							}
						}
					}
					else if (scanFilter instanceof HBaseMergingFilter) {
						HBaseMergingFilter mergingFilter = (HBaseMergingFilter) scanFilter;
						mergeData = mergingFilter.getMergeData();
					}
				}
			}

			if (mergeData != null) {
				LOGGER.debug(
						">> PostScannerNext got data from merging filter: " + mergeData);

				if (results.size() > 1) {
					LOGGER.debug(
							">> PostScannerNext has " + results.size() + " rows");

					boolean mergeMe = false;
					byte[] curRow = null;

					for (Result result : results) {
						byte[] row = result.getRow();

						if (row != null) {
							if (Bytes.equals(
									row,
									curRow)) {
								mergeMe = true;
							}

							curRow = Bytes.copy(
									row);
						}
					}

					if (mergeMe) LOGGER.debug(
							">> PostScannerNext has mergeable rows");
				}
			}
		}

		return hasMore;
	}

}
