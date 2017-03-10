package mil.nga.giat.geowave.core.store.entities;

/**
 * There is a single intermediate row per original entry passed into a write
 * operation. This offers a higher level abstraction from the raw key-value
 * pairs in geowave (can be multiple per original entry). A datastore is
 * responsible for translating from intermediate rows to key-value rows.
 *
 */
public class IntermediateGeoWaveRow
{

}
