package mil.nga.giat.geowave.adapter.vector.index;

import java.io.IOException;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

public class ChooseAllMatchIndexQueryStrategy implements
		IndexQueryStrategy
{

	@Override
	public CloseableIterator<Index<?, ?>> getIndices(
			final Constraints timeConstraints,
			final Constraints geoConstraints,
			final CloseableIterator<Index<?, ?>> indices ) {
		return new CloseableIterator<Index<?, ?>>() {
			Index<?, ?> nextIdx = null;

			@Override
			public boolean hasNext() {
				while (nextIdx == null && indices.hasNext()) {
					nextIdx = indices.next();
				} 
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
