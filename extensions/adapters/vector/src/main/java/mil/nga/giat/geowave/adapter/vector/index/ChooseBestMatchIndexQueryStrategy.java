package mil.nga.giat.geowave.adapter.vector.index;

import java.io.IOException;

import mil.nga.giat.geowave.core.geotime.DimensionalityType;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

public class ChooseBestMatchIndexQueryStrategy implements
		IndexQueryStrategy
{

	@Override
	public CloseableIterator<Index<?, ?>> getIndices(
			final Constraints timeConstraints,
			final Constraints geoConstraints,
			final CloseableIterator<Index<?, ?>> indices ) {
		return new CloseableIterator<Index<?, ?>>() {
			PrimaryIndex nextIdx = null;
			boolean done = false;

			@Override
			public boolean hasNext() {
				while (!done && indices.hasNext()) {
					Index<?, ?> nextChoosenIdx = indices.next();
					if (nextChoosenIdx instanceof PrimaryIndex) {
						nextIdx = (PrimaryIndex) nextChoosenIdx;
						if (!timeConstraints.isEmpty() && DimensionalityType.SPATIAL_TEMPORAL.isCompatible(nextIdx)) break;
						if (timeConstraints.isEmpty() && DimensionalityType.SPATIAL.isCompatible(nextIdx)) break;;
					}
				}
				done = true;
				return nextIdx == null;
			}

			@Override
			public Index<?, ?> next() {
				Index<?, ?> returnVal = nextIdx;
				nextIdx = null;
				return returnVal;
			}

			@Override
			public void close()
					throws IOException {
				indices.close();
			}
		};
	}
}
