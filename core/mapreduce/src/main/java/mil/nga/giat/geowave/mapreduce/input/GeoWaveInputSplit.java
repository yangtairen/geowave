package mil.nga.giat.geowave.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * The Class GeoWaveInputSplit. Encapsulates a GeoWave Index and a set of ranges
 * for use in Map Reduce jobs.
 */
public class GeoWaveInputSplit extends
		InputSplit implements
		Writable
{
	private Map<Index, List<ByteArrayRange>> ranges;
	private String[] locations;

	protected GeoWaveInputSplit() {
		ranges = new HashMap<Index, List<ByteArrayRange>>();
		locations = new String[] {};
	}

	protected GeoWaveInputSplit(
			final Map<Index, List<ByteArrayRange>> ranges,
			final String[] locations ) {
		this.ranges = ranges;
		this.locations = locations;
	}

	public Set<Index> getIndices() {
		return ranges.keySet();
	}

	public List<ByteArrayRange> getRanges(
			final Index index ) {
		return ranges.get(index);
	}

	/**
	 * This implementation of length is only an estimate, it does not provide
	 * exact values. Do not have your code rely on this return value.
	 */
	@Override
	public long getLength()
			throws IOException {
		long diff = 0;
		for (final Entry<Index, List<ByteArrayRange>> indexEntry : ranges.entrySet()) {
			for (final ByteArrayRange range : indexEntry.getValue()) {
				final byte[] start = range.getStart().getBytes();
				final byte[] stop = range.getEnd().getBytes();
				final int maxCommon = Math.min(
						7,
						Math.min(
								start.length,
								stop.length));
				for (int i = 0; i < maxCommon; ++i) {
					diff |= 0xff & (start[i] ^ stop[i]);
					diff <<= Byte.SIZE;
				}

				if (start.length != stop.length) {
					diff |= 0xff;
				}
			}
		}
		return diff + 1;
	}

	@Override
	public String[] getLocations()
			throws IOException {
		return locations;
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		final int numIndices = in.readInt();
		ranges = new HashMap<Index, List<ByteArrayRange>>(
				numIndices);
		for (int i = 0; i < numIndices; i++) {
			final int indexLength = in.readInt();
			final byte[] indexBytes = new byte[indexLength];
			in.readFully(indexBytes);
			final Index index = PersistenceUtils.fromBinary(
					indexBytes,
					Index.class);
			final int numRanges = in.readInt();
			final List<ByteArrayRange> rangeList = new ArrayList<ByteArrayRange>(
					numRanges);

			for (int j = 0; j < numRanges; j++) {
				int length = in.readInt();
				final byte[] start = new byte[length];
				in.readFully(start);
				length = in.readInt();
				final byte[] end = new byte[length];
				in.readFully(end);
				rangeList.add(new ByteArrayRange(
						new ByteArrayId(
								start),
						new ByteArrayId(
								end)));
			}
			ranges.put(
					index,
					rangeList);
		}
		final int numLocs = in.readInt();
		locations = new String[numLocs];
		for (int i = 0; i < numLocs; ++i) {
			locations[i] = in.readUTF();
		}
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		out.writeInt(ranges.size());
		for (final Entry<Index, List<ByteArrayRange>> range : ranges.entrySet()) {
			final byte[] indexBytes = PersistenceUtils.toBinary(range.getKey());
			out.writeInt(indexBytes.length);
			out.write(indexBytes);
			final List<ByteArrayRange> rangeList = range.getValue();
			out.writeInt(rangeList.size());
			for (final ByteArrayRange r : rangeList) {
				final byte[] start = r.getStart().getBytes();
				final byte[] end = r.getEnd().getBytes();
				out.writeInt(start.length);
				out.write(start);
				out.writeInt(end.length);
				out.write(end);
			}
		}
		out.writeInt(locations.length);
		for (final String location : locations) {
			out.writeUTF(location);
		}
	}
}
