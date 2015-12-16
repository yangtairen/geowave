package mil.nga.giat.geowave.adapter.vector.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;

public class ChooseHeuristicMatchIndexQueryStrategy implements
		IndexQueryStrategySPI
{
	public static final String NAME = "Heuristic Match";

	public String toString() {
		return NAME;
	}

	@Override
	public CloseableIterator<Index<?, ?>> getIndices(
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
			final BasicQuery query,
			final CloseableIterator<Index<?, ?>> indices ) {
		return new CloseableIterator<Index<?, ?>>() {
			PrimaryIndex nextIdx = null;
			boolean done = false;

			@Override
			public boolean hasNext() {
				double min = Long.MAX_VALUE;
				PrimaryIndex bestIdx = null;
				while (!done && indices.hasNext()) {
					final Index<?, ?> nextChoosenIdx = indices.next();
					if (nextChoosenIdx instanceof PrimaryIndex) {
						nextIdx = (PrimaryIndex) nextChoosenIdx;
						final List<MultiDimensionalNumericData> queryRanges = query.getIndexConstraints(nextIdx.getIndexStrategy());
						final double rangePerDimension[] = nextIdx.getIndexStrategy().getHighestPrecisionIdRangePerDimension();
						long totalMax = 0;
						for (MultiDimensionalNumericData qr : queryRanges) {
							long maxCell = Long.MIN_VALUE;
							for (int d = 0; d < rangePerDimension.length; d++) {
								double temp = (qr.getMaxValuesPerDimension()[d] - qr.getMinValuesPerDimension()[d]);
								maxCell = Math.max(
										(long) Math.ceil(temp / rangePerDimension[d]),
										maxCell);
							}
							totalMax += maxCell;
						}
						double temp = Math.pow(
								totalMax,
								rangePerDimension.length);
						if (temp < min) {
							min = temp;
							bestIdx = nextIdx;
						}
					}
				}
				nextIdx = bestIdx;
				done = true;
				return nextIdx != null;
			}

			@Override
			public Index<?, ?> next()
					throws NoSuchElementException {
				if (nextIdx == null) throw new NoSuchElementException();
				Index<?, ?> returnVal = nextIdx;
				nextIdx = null;
				return returnVal;
			}

			@Override
			public void remove() {}

			@Override
			public void close()
					throws IOException {
				indices.close();
			}
		};
	}
}
