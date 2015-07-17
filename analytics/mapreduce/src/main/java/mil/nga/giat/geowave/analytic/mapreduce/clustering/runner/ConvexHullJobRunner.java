package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.ConvexHullMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.StoreParameters;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.geotools.feature.type.BasicFeatureTypes;

/**
 *

 */
public class ConvexHullJobRunner extends
		GeoWaveAnalyticJobRunner
{

	private int zoomLevel = 1;

	public ConvexHullJobRunner() {
		super.setOutputFormatConfiguration(new GeoWaveOutputFormatConfiguration());
	}

	public void setZoomLevel(
			final int zoomLevel ) {
		this.zoomLevel = zoomLevel;
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {
		job.setMapperClass(ConvexHullMapReduce.ConvexHullMap.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setReducerClass(ConvexHullMapReduce.ConvexHullReducer.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(Object.class);
	}

	private DataAdapter<?> getAdapter(
			final PropertyManagement runTimeProperties )
			throws Exception {

		final String projectionDataTypeId = runTimeProperties.storeIfEmpty(
				HullParameters.Hull.DATA_TYPE_ID,
				"convex_hull").toString();

		final AdapterStore adapterStore = super.getAdapterStore(runTimeProperties);

		DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
				projectionDataTypeId));

		if (adapter == null) {
			final String namespaceURI = runTimeProperties.storeIfEmpty(
					HullParameters.Hull.DATA_NAMESPACE_URI,
					BasicFeatureTypes.DEFAULT_NAMESPACE).toString();
			adapter = AnalyticFeature.createGeometryFeatureAdapter(
					projectionDataTypeId,
					new String[0],
					namespaceURI,
					ClusteringUtils.CLUSTERING_CRS);
			adapterStore.addAdapter(adapter);
		}
		return adapter;

	}

	private void checkIndex(
			final PropertyManagement runTimeProperties )
			throws Exception {

		final String indexId = runTimeProperties.getPropertyAsString(
				HullParameters.Hull.INDEX_ID,
				runTimeProperties.getPropertyAsString(CentroidParameters.Centroid.INDEX_ID));

		final IndexStore indexStore = super.getIndexStore(runTimeProperties);

		Index index = indexStore.getIndex(new ByteArrayId(
				indexId));
		if (index == null) {
			index = new CustomIdIndex(
					IndexType.SPATIAL_VECTOR.createDefaultIndexStrategy(),
					IndexType.SPATIAL_VECTOR.getDefaultIndexModel(),
					new ByteArrayId(
							indexId));
			indexStore.addIndex(index);

		}
	}

	@Override
	public Class<?> getScope() {
		return ConvexHullMapReduce.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				HullParameters.Hull.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);
		runTimeProperties.storeIfEmpty(
				HullParameters.Hull.PROJECTION_CLASS,
				SimpleFeatureProjection.class);
		runTimeProperties.setConfig(
				new ParameterEnum<?>[] {
					HullParameters.Hull.WRAPPER_FACTORY_CLASS,
					HullParameters.Hull.PROJECTION_CLASS,
					HullParameters.Hull.DATA_TYPE_ID,
					HullParameters.Hull.INDEX_ID
				},
				config,
				getScope());
		setReducerCount(runTimeProperties.getPropertyAsInt(
				HullParameters.Hull.REDUCER_COUNT,
				4));
		CentroidManagerGeoWave.setParameters(
				config,
				getScope(),
				runTimeProperties);
		NestedGroupCentroidAssignment.setParameters(
				config,
				getScope(),
				runTimeProperties);

		final int localZoomLevel = runTimeProperties.getPropertyAsInt(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				zoomLevel);
		// getting group from next level, now that the prior level is complete
		NestedGroupCentroidAssignment.setZoomLevel(
				config,
				getScope(),
				localZoomLevel + 1);

		addDataAdapter(
				config,
				getAdapter(runTimeProperties));
		checkIndex(runTimeProperties);

		return super.run(
				config,
				runTimeProperties);
	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(super.getParameters());

		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			StoreParameters.StoreParam.DATA_STORE,
			GlobalParameters.Global.BATCH_ID
		}));

		params.addAll(MapReduceParameters.getParameters());
		params.addAll(NestedGroupCentroidAssignment.getParameters());

		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			HullParameters.Hull.WRAPPER_FACTORY_CLASS,
			HullParameters.Hull.PROJECTION_CLASS,
			HullParameters.Hull.REDUCER_COUNT,
			HullParameters.Hull.DATA_TYPE_ID,
			HullParameters.Hull.DATA_NAMESPACE_URI,
			HullParameters.Hull.INDEX_ID
		}));
		return params;
	}

}