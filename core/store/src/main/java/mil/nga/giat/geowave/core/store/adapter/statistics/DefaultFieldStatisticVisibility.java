package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;

/**
 * This assigns the visibility of the key-value with the most-significant field
 * bitmask (the first fields in the bitmask are the indexed fields, and all
 * indexed fields should be the default visibility which should be the minimal
 * set of visibility contraints of any field)
 *
 * @param <T>
 *            The field type
 */
public class DefaultFieldStatisticVisibility<T> implements
		EntryVisibilityHandler<T>
{

	@Override
	public byte[] getVisibility(
			final T entry,
			final GeoWaveRow... kvs ) {
		if (kvs.length == 1) {
			return kvs[0].getVisibility();
		}
		else if (kvs.length > 1) {
			int lowestOrdinal = Integer.MAX_VALUE;
			byte[] lowestOrdinalVisibility = null;
			for (final GeoWaveRow kv : kvs) {
				final int pos = BitmaskUtils.getLowestFieldPosition(
						kv.getFieldMask());
				if (pos == 0) {
					return kv.getVisibility();
				}
				if (pos <= lowestOrdinal) {
					lowestOrdinal = pos;
					lowestOrdinalVisibility = kv.getVisibility();
				}
			}
			return lowestOrdinalVisibility;
		}

		return null;
	}

}
