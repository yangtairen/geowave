package mil.nga.giat.geowave.analytic.mapreduce.nn;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.geotools.feature.type.BasicFeatureTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.IndependentJobRunner;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.GeoWaveInputLoadJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce.PassthruPartitioner;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters.Clustering;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters.Partition;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;

/**
 * NN Neighbor Intersection
 */

public class NNIntersectionSuiteRunner implements
		MapReduceJobRunner,
		IndependentJobRunner
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(NNIntersectionSuiteRunner.class);
	protected final NNIntersectionJobRunner jobRunner = new NNIntersectionJobRunner();
	protected final NNIntersectionMergeRunner mergeRunnner = new NNIntersectionMergeRunner();
	protected FormatConfiguration inputFormatConfiguration;
	protected int zoomLevel = 1;

	public NNIntersectionSuiteRunner() {
		super();
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		final String outputBaseDir = runTimeProperties.getPropertyAsString(
				MapReduceParameters.MRConfig.HDFS_BASE_DIR,
				"/tmp");

		Path exchangePath = new Path(
				outputBaseDir + "/run" + UUID.randomUUID().toString());
		jobRunner.setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration(
				exchangePath));
		final int initialStatus = jobRunner.run(
				config,
				runTimeProperties);

		if (initialStatus != 0) {
			return initialStatus;
		}

		mergeRunnner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				exchangePath));
		mergeRunnner.setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration());

		return mergeRunnner.run(
				config,
				runTimeProperties);

	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(jobRunner.getParameters());
		return params;
	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				MapReduceJobController.getConfiguration(runTimeProperties),
				runTimeProperties);
	}

}