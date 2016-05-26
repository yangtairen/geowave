package mil.nga.giat.geowave.analytic.mapreduce.nn;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mil.nga.giat.geowave.analytic.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce.NNReducer;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce.PartitionDataWritable;
import mil.nga.giat.geowave.analytic.nn.DistanceProfile;
import mil.nga.giat.geowave.analytic.nn.NeighborList;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.feature.simple.SimpleFeature;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Efficiently, Determine if a points are close to protected resources
 * (polygons).
 * 
 * The result of this reducer is Text,Text The text key is the feature id of the
 * protected resource. The text value is a comma separated list of feature ids
 * found near the resource. The result should be consumed by IDReducer to merge
 * across multiple partitions.
 * 
 * Note: I did not use the NNProcessor here. The NNProcessor does secondary
 * partitioning and removes redundant geometries. However, NNProcessor currently
 * does distinguish between the two types of features.
 * 
 * 
 */
public class NNIntersection
{
	public static class NNIntersectionReducer extends
			NNReducer<SimpleFeature, Text, Text, List<SimpleFeature>>
	{

		@Override
		protected void reduce(
				final PartitionDataWritable key,
				final Iterable<AdapterWithObjectWritable> values,
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, Text, Text>.Context context )
				throws IOException,
				InterruptedException {

			Map<SimpleFeature, List<String>> protectedSet = new HashMap<SimpleFeature, List<String>>();
			Set<SimpleFeature> inspectionSet = new HashSet<SimpleFeature>();

			for (final AdapterWithObjectWritable inputValue : values) {
				final SimpleFeature value = (SimpleFeature) AdapterWithObjectWritable.fromWritableWithAdapter(
						serializationTool,
						inputValue);
				// determine the element is protected...assume all non simple
				// (points) are protected
				// MAY WANT TO COME UP WITH A BETTER WAY
				if (((Geometry) value.getDefaultGeometry()).isSimple()) {
					inspectionSet.add(value);
				}
				else {
					protectedSet.put(
							value,
							new LinkedList());
				}
			}

			// compare inspected to protected
			for (SimpleFeature inspectElement : inspectionSet) {

				for (Entry<SimpleFeature, List<String>> protectedEntry : protectedSet.entrySet()) {
					final DistanceProfile<?> distanceProfile = distanceProfileFn.computeProfile(
							inspectElement,
							protectedEntry.getKey());
					if (distanceProfile.getDistance() <= this.maxDistance) {
						// too close...so record it
						protectedEntry.getValue().add(
								inspectElement.getID());
					}
				}
			}

			for (Entry<SimpleFeature, List<String>> protectedEntry : protectedSet.entrySet()) {
				this.sendToOutput(
						protectedEntry.getKey(),
						protectedEntry.getValue(),
						context);
			}

		}

		final Text primaryText = new Text();
		final Text neighborsText = new Text();
		final byte[] sepBytes = new byte[] {
			0x2c
		};

		protected void sendToOutput(
				final SimpleFeature primary,
				final List<String> neighbors,
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, Text, Text>.Context context )
				throws IOException,
				InterruptedException {
			if ((neighbors == null) || (neighbors.size() == 0)) {
				return;
			}
			primaryText.clear();
			neighborsText.clear();
			byte[] utfBytes;
			try {

				utfBytes = primary.getID().getBytes(
						"UTF-8");
				primaryText.append(
						utfBytes,
						0,
						utfBytes.length);
				for (String neighbor : neighbors) {
					if (neighborsText.getLength() > 0) {
						neighborsText.append(
								sepBytes,
								0,
								sepBytes.length);
					}
					utfBytes = neighbor.getBytes("UTF-8");
					neighborsText.append(
							utfBytes,
							0,
							utfBytes.length);
				}

				context.write(
						primaryText,
						neighborsText);
			}
			catch (final UnsupportedEncodingException e) {
				throw new RuntimeException(
						"UTF-8 Encoding invalid for Simople feature ID",
						e);
			}

		}

		@Override
		protected List<SimpleFeature> createSummary() {
			return null;
		}

		@Override
		protected void processSummary(
				PartitionData partitionData,
				List<SimpleFeature> summary,
				Reducer<PartitionDataWritable, AdapterWithObjectWritable, Text, Text>.Context context )
				throws IOException,
				InterruptedException {

		}

		@Override
		protected void processNeighbors(
				PartitionData partitionData,
				ByteArrayId primaryId,
				SimpleFeature primary,
				NeighborList<SimpleFeature> neighbors,
				Reducer<PartitionDataWritable, AdapterWithObjectWritable, Text, Text>.Context context,
				List<SimpleFeature> summary )
				throws IOException,
				InterruptedException {

		}

	}

	public class IDReducer extends
			Reducer<Text, Text, Text, Text>
	{

		final Text output = new Text();

		@Override
		protected void reduce(
				Text protectedFid,
				Iterable<Text> fidLists,
				Context context )
				throws IOException,
				InterruptedException {
			Set<String> fidSet = new HashSet<String>();

			for (Text fidText : fidLists) {
				String fids[] = fidText.toString().split(
						",");
				for (String fid : fids)
					fidSet.add(fid);
			}
			output.set(Joiner.on(
					",").join(
					fidSet));
			context.write(
					protectedFid,
					output);
		}

	}

}