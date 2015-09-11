package mil.nga.giat.geowave.core.store.filter;

import java.util.List;

import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

/**
 * This class wraps a list of filters into a single filter such that if any one
 * filter fails this class will fail acceptance.
 * 
 * @param <T>
 */
public class FilterList<T extends QueryFilter> implements
		QueryFilter
{
	protected List<T> filters;

	protected FilterList() {}

	public FilterList(
			final List<T> filters ) {
		this.filters = filters;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding entry ) {
		for (final QueryFilter filter : filters) {
			if (!filter.accept(
					indexModel,
					entry)) {
				return false;
			}
		}
		return true;
	}

}
