package mil.nga.giat.geowave.analytic.clustering;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.StoreParameters;
import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;

import org.geotools.feature.type.BasicFeatureTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Polygon;

public class ClusteringUtils
{

	public static final String CLUSTERING_CRS = "EPSG:4326";

	final static Logger LOGGER = LoggerFactory.getLogger(ClusteringUtils.class);

	private static Index createIndex(
			final String indexId,
			final IndexStore indexStore )
			throws Exception {

		final ByteArrayId dbId = new ByteArrayId(
				indexId);
		if (!indexStore.indexExists(dbId)) {
			if (indexId.equals(IndexType.SPATIAL_VECTOR.getDefaultId())) {
				final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
				indexStore.addIndex(index);
				return index;
			}
			else if (indexId.equals(IndexType.SPATIAL_TEMPORAL_VECTOR.getDefaultId())) {
				final Index index = IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex();
				indexStore.addIndex(index);
				return index;
			}
			else {
				final Index index = new CustomIdIndex(
						IndexType.SPATIAL_VECTOR.createDefaultIndexStrategy(),
						IndexType.SPATIAL_VECTOR.getDefaultIndexModel(),
						new ByteArrayId(
								indexId));
				indexStore.addIndex(index);
				return index;
			}
		}
		else {
			return indexStore.getIndex(dbId);
		}

	}

	private static DataAdapter<?> createAdapter(
			final String sampleDataTypeId,
			final String sampleDataNamespaceURI,
			final AdapterStore adapterStore,
			final String[] dimensionNames )
			throws Exception {

		final FeatureDataAdapter adapter = AnalyticFeature.createGeometryFeatureAdapter(
				sampleDataTypeId,
				dimensionNames,
				sampleDataNamespaceURI,
				CLUSTERING_CRS);

		final ByteArrayId dbId = new ByteArrayId(
				sampleDataTypeId);
		if (!adapterStore.adapterExists(dbId)) {
			adapterStore.addAdapter(adapter);
			return adapter;
		}
		else {
			return adapterStore.getAdapter(dbId);
		}

	}

	public static DataAdapter[] getAdapters(
			final PropertyManagement propertyManagement )
			throws IOException {
		final AdapterStore adapterStore = ((AdapterStoreCommandLineOptions) StoreParameters.StoreParam.ADAPTER_STORE.getHelper().getValue(
				propertyManagement)).createStore();

		final mil.nga.giat.geowave.core.store.CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
		final List<DataAdapter> adapters = new LinkedList<DataAdapter>();
		while (it.hasNext()) {
			adapters.add(it.next());
		}

		final DataAdapter[] result = new DataAdapter[adapters.size()];
		adapters.toArray(result);
		return result;
	}

	public static Index[] getIndices(
			final PropertyManagement propertyManagement ) {

		final IndexStore indexStore = ((IndexStoreCommandLineOptions) StoreParameters.StoreParam.INDEX_STORE.getHelper().getValue(
				propertyManagement)).createStore();

		final mil.nga.giat.geowave.core.store.CloseableIterator<Index> it = indexStore.getIndices();
		final List<Index> indices = new LinkedList<Index>();
		while (it.hasNext()) {
			indices.add(it.next());
		}

		final Index[] result = new Index[indices.size()];
		indices.toArray(result);
		return result;
	}

	/*
	 * Method takes in a polygon and generates the corresponding ranges in a
	 * GeoWave spatial index
	 */
	protected static List<ByteArrayRange> getGeoWaveRangesForQuery(
			final Polygon polygon ) {

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final List<ByteArrayRange> ranges = index.getIndexStrategy().getQueryRanges(
				new SpatialQuery(
						polygon).getIndexConstraints(index.getIndexStrategy()));

		return ranges;
	}

	public static Index createIndex(
			final PropertyManagement propertyManagement )
			throws Exception {

		final IndexStore indexStore = ((IndexStoreCommandLineOptions) StoreParameters.StoreParam.INDEX_STORE.getHelper().getValue(
				propertyManagement)).createStore();
		return indexStore.getIndex(new ByteArrayId(
				propertyManagement.getPropertyAsString(CentroidParameters.Centroid.INDEX_ID)));
	}

	public static DataAdapter<?> createAdapter(
			final PropertyManagement propertyManagement )
			throws Exception {

		final Class<DimensionExtractor> dimensionExtractorClass = propertyManagement.getPropertyAsClass(
				CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
				DimensionExtractor.class);

		return ClusteringUtils.createAdapter(
				propertyManagement.getPropertyAsString(CentroidParameters.Centroid.DATA_TYPE_ID),
				propertyManagement.getPropertyAsString(
						CentroidParameters.Centroid.DATA_NAMESPACE_URI,
						BasicFeatureTypes.DEFAULT_NAMESPACE),
				((AdapterStoreCommandLineOptions) StoreParameters.StoreParam.ADAPTER_STORE.getHelper().getValue(
						propertyManagement)).createStore(),
				dimensionExtractorClass.newInstance().getDimensionNames());
	}
}
