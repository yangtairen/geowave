package mil.nga.giat.geowave.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

public interface MapReduceDataStore
{
	public List<InputSplit> getSplits(
			final JobContext context )
			throws IOException,
			InterruptedException;
}
