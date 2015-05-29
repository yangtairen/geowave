package mil.nga.giat.geowave.mapreduce;

import java.util.List;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;

import org.apache.hadoop.mapreduce.InputSplit;

public interface MapReduceDataStore extends
		DataStore
{
	public List<InputSplit> getSplits(
			Index[] indices,
			DistributableQuery query,
			String geowaveNamespace,
			final Integer minSplits,
			final Integer maxSplits );
}
