package mil.nga.giat.geowave.core.store.index;

import java.util.List;

import mil.nga.giat.geowave.core.index.SortedIndexStrategy;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;

/**
 * Interface which defines an index strategy.
 * 
 */
public interface FieldIndexStrategy<ConstraintType extends FilterableConstraints, FieldType> extends
		SortedIndexStrategy<ConstraintType, List<FieldInfo<FieldType>>>
{

}
