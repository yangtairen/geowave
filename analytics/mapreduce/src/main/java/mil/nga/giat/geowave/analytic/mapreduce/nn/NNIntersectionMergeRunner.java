package mil.nga.giat.geowave.analytic.mapreduce.nn;

import mil.nga.giat.geowave.analytic.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce.PartitionDataWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class NNIntersectionMergeRunner extends
		GeoWaveAnalyticJobRunner
{
	public NNIntersectionMergeRunner() {
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
	protected String getJobName() {
		return "Nearest Neighbors Intersection Merge";
	}
}