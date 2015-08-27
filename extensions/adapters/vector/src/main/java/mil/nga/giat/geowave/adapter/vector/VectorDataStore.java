package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.adapter.vector.render.DistributableRenderer;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.Query;

import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

/**
 * This is the most convenient way of using GeoWave as a SimpleFeature
 * persistence store. Beyond the basic functions of DataStore (general-purpose
 * query and ingest of any data adapter type) this class enables distributed
 * rendering and decimation on SimpleFeature, both with CQL filtering able to be
 * applied to features server.
 * 
 */
public interface VectorDataStore
{

	@SuppressWarnings("unchecked")
	public CloseableIterator<SimpleFeature> query(
			final FeatureDataAdapter adapter,
			final Index index,
			final Query query,
			final Filter filter,
			final Integer limit,
			final String... authorizations );

	public CloseableIterator<SimpleFeature> query(
			final FeatureDataAdapter adapter,
			final Index index,
			final Query query,
			final Filter filter,
			final DistributableRenderer distributedRenderer,
			final String... authorizations );

	@SuppressWarnings("unchecked")
	public CloseableIterator<SimpleFeature> query(
			final FeatureDataAdapter adapter,
			final Index index,
			final Query query,
			final int width,
			final int height,
			final double pixelSize,
			final Filter filter,
			final ReferencedEnvelope envelope,
			final Integer limit,
			final String... authorizations );
}
