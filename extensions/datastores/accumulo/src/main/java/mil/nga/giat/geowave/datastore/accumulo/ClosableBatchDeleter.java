package mil.nga.giat.geowave.datastore.accumulo;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.Deleter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;

public class ClosableBatchDeleter implements
		Deleter<AccumuloRow>
{
	private static Logger LOGGER = LoggerFactory.getLogger(
			ClosableBatchDeleter.class);
	private final BatchDeleter deleter;
	private final boolean isAltIndex;

	public ClosableBatchDeleter(
			final BatchDeleter deleter,
			final boolean isAltIndex ) {
		this.deleter = deleter;
		this.isAltIndex = isAltIndex;
	}

	@Override
	public void close() {
		deleter.close();
	}

	public BatchDeleter getDeleter() {
		return deleter;
	}

	@Override
	public void delete(
			final AccumuloRow row,
			final DataAdapter<?> adapter ) {
		final List<Range> rowRanges = new ArrayList<Range>();
		if (isAltIndex) {
			rowRanges.add(
					Range.exact(
							new Text(
									row.getDataId())));
		}
		else {
			rowRanges.add(
					Range.exact(
							new Text(
									GeoWaveKey.getCompositeId(
											row))));
		}
		final BatchDeleter batchDeleter = getDeleter();
		batchDeleter.setRanges(
				rowRanges);
		try {
			batchDeleter.delete();
		}
		catch (MutationsRejectedException | TableNotFoundException e) {
			LOGGER.warn(
					"Unable to delete row: " + row.toString(),
					e);
		}
	}
}
