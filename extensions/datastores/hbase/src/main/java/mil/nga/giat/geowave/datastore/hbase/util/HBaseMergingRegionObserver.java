package mil.nga.giat.geowave.datastore.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseMergingFilter;

public class HBaseMergingRegionObserver extends
		BaseRegionObserver
{
	private final static Logger LOGGER = Logger.getLogger(HBaseMergingRegionObserver.class);

	// TEST ONLY!
	static {
		LOGGER.setLevel(Level.DEBUG);
	}

	private HashMap<RegionScanner, HBaseMergingFilter> filterMap = new HashMap<RegionScanner, HBaseMergingFilter>();

	@Override
	public InternalScanner preFlush(
			ObserverContext<RegionCoprocessorEnvironment> e,
			Store store,
			InternalScanner scanner )
			throws IOException {
		LOGGER.debug(">>> preFlush...");

		if (scanner instanceof StoreScanner) {
			StoreScanner storeScanner = (StoreScanner) scanner;

			List<Cell> prevCells = new ArrayList<Cell>();
			storeScanner.next(prevCells);

			LOGGER.debug("First row has " + prevCells.size() + " cells");

			List<Cell> rowCells = new ArrayList<Cell>();
			while (storeScanner.next(rowCells)) {
				LOGGER.debug("Next row has " + rowCells.size() + " cells");

				int cellNum = 0;
				for (final Cell prevCell : prevCells) {
					final Cell rowCell = rowCells.get(cellNum);

					final GeoWaveRow prevId = new GeoWaveRowImpl(
							prevCell.getRowArray());

					final GeoWaveRow rowId = new GeoWaveRowImpl(
							rowCell.getRowArray());

					if (DataStoreUtils.rowIdsMatch(
							rowId,
							prevId)) {
						LOGGER.debug("MERGING CELL " + cellNum + " for RowID = "
								+ StringUtils.stringFromBinary(rowId.getDataId()));
					}
					
					cellNum++;
				}
				
				prevCells.clear();
				prevCells.addAll(rowCells);
				rowCells.clear();
			}
		}

		return scanner;
	}

	@Override
	public RegionScanner postScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan,
			final RegionScanner s )
			throws IOException {
		if (scan != null) {
			Filter scanFilter = scan.getFilter();
			if (scanFilter != null) {
				HBaseMergingFilter mergingFilter = extractMergingFilter(scanFilter);

				if (mergingFilter != null) {
					filterMap.put(
							s,
							mergingFilter);
				}
			}
		}

		return s;
	}

	private HBaseMergingFilter extractMergingFilter(
			Filter checkFilter ) {
		if (checkFilter instanceof HBaseMergingFilter) {
			return (HBaseMergingFilter) checkFilter;
		}

		if (checkFilter instanceof FilterList) {
			for (Filter filter : ((FilterList) checkFilter).getFilters()) {
				HBaseMergingFilter mergingFilter = extractMergingFilter(filter);
				if (mergingFilter != null) {
					return mergingFilter;
				}
			}
		}

		return null;
	}

	@Override
	public boolean preScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s,
			final List<Result> results,
			final int limit,
			final boolean hasMore )
			throws IOException {
		HBaseMergingFilter mergingFilter = filterMap.get(s);

		if (mergingFilter != null) {
			// TODO: Any pre-scan work?
		}

		return hasMore;
	}

	@Override
	public boolean postScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s,
			final List<Result> results,
			final int limit,
			final boolean hasMore )
			throws IOException {
		HBaseMergingFilter mergingFilter = filterMap.get(s);

		if (results.size() > 0) {
			String mergeData = null;

			if (mergingFilter != null) {
				mergeData = mergingFilter.getMergeData();
			}

			if (mergeData != null) {
				LOGGER.debug(">> PostScannerNext got data from merging filter: " + mergeData);

				if (results.size() > 1) {
					LOGGER.debug(">> PostScannerNext has " + results.size() + " rows");

					HashMap<String, List<Result>> rowMap = new HashMap<String, List<Result>>();

					for (Result result : results) {
						byte[] row = result.getRow();

						if (row != null) {
							String rowKey = StringUtils.stringFromBinary(row);
							List<Result> resultList = rowMap.get(rowKey);

							if (resultList == null) {
								resultList = new ArrayList<Result>();
							}

							resultList.add(result);
							rowMap.put(
									rowKey,
									resultList);
						}
					}

					if (!rowMap.isEmpty()) {
						LOGGER.debug(">> PostScannerNext got " + rowMap.keySet().size() + " unique rows");
						for (String rowKey : rowMap.keySet()) {
							List<Result> resultList = rowMap.get(rowKey);
							LOGGER.debug(">> PostScannerNext got " + resultList.size() + " results for row " + rowKey);
						}
					}
				}
			}
		}

		return hasMore;
	}

}
