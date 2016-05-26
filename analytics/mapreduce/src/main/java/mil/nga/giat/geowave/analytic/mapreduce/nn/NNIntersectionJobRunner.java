package mil.nga.giat.geowave.analytic.mapreduce.nn;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.analytic.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce.PartitionDataWritable;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce.PassthruPartitioner;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters.Partition;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class NNIntersectionJobRunner extends
		GeoWaveAnalyticJobRunner
{
	public NNIntersectionJobRunner() {
		setInputFormatConfiguration(new GeoWaveInputFormatConfiguration());
		setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration());
		super.setReducerCount(4);
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {
		job.setMapperClass(NNMapReduce.NNMapper.class);
		job.setReducerClass(NNIntersection.NNIntersectionReducer.class);
		job.setMapOutputKeyClass(PartitionDataWritable.class);
		job.setMapOutputValueClass(AdapterWithObjectWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setSpeculativeExecution(false);
	}

	@Override
	public Class<?> getScope() {
		return NNMapReduce.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		final Partitioner<?> partitioner = runTimeProperties.getClassInstance(
				Partition.PARTITIONER_CLASS,
				Partitioner.class,
				OrthodromicDistancePartitioner.class);

		partitioner.setup(
				runTimeProperties,
				getScope(),
				config);

		runTimeProperties.setConfig(
				new ParameterEnum[] {
					Partition.PARTITIONER_CLASS,
					Partition.MAX_DISTANCE,
					Partition.MAX_MEMBER_SELECTION,
					Partition.GEOMETRIC_DISTANCE_UNIT,
					Partition.DISTANCE_THRESHOLDS,
					CommonParameters.Common.DISTANCE_FUNCTION_CLASS
				},
				config,
				getScope());

		return super.run(
				config,
				runTimeProperties);

	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(super.getParameters());
		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			Partition.PARTITIONER_CLASS,
			Partition.MAX_DISTANCE,
			Partition.MAX_MEMBER_SELECTION,
			Partition.GEOMETRIC_DISTANCE_UNIT,
			Partition.DISTANCE_THRESHOLDS,
			CommonParameters.Common.DISTANCE_FUNCTION_CLASS
		}));
		return params;
	}

	@Override
	protected String getJobName() {
		return "Nearest Neighbors Intersection";
	}
}