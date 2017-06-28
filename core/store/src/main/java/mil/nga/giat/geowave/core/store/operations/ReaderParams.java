package mil.nga.giat.geowave.core.store.operations;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

public class ReaderParams
{
	private final PrimaryIndex index;
	private final List<ByteArrayId> adapterIds;
	private final double[] maxResolutionSubsamplingPerDimension;
	private final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
	private final Pair<List<String>, DataAdapter<?>> fieldSubsets;
	private final boolean isWholeRow;
	private final QueryRanges queryRanges;
	private final DistributableQueryFilter filter;
	private final Integer limit;
	private List<MultiDimensionalCoordinateRangesArray> coordinateRanges;
	private List<MultiDimensionalNumericData> constraints;
	private final String[] additionalAuthorizations;

	public ReaderParams(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldSubsets,
			final boolean isWholeRow,
			final QueryRanges queryRanges,
			final DistributableQueryFilter filter,
			final Integer limit,
			final List<MultiDimensionalCoordinateRangesArray> coordinateRanges,
			final List<MultiDimensionalNumericData> constraints,
			final String... additionalAuthorizations ) {
		this.index = index;
		this.adapterIds = adapterIds;
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
		this.aggregation = aggregation;
		this.fieldSubsets = fieldSubsets;
		this.isWholeRow = isWholeRow;
		this.queryRanges = queryRanges;
		this.filter = filter;
		this.limit = limit;
		this.coordinateRanges = coordinateRanges;
		this.constraints = constraints;
		this.additionalAuthorizations = additionalAuthorizations;
	}

	public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		return coordinateRanges;
	}

	public List<MultiDimensionalNumericData> getConstraints() {
		return constraints;
	}

	public PrimaryIndex getIndex() {
		return index;
	}

	public List<ByteArrayId> getAdapterIds() {
		return adapterIds;
	}

	public double[] getMaxResolutionSubsamplingPerDimension() {
		return maxResolutionSubsamplingPerDimension;
	}

	public Pair<DataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return aggregation;
	}

	public Pair<List<String>, DataAdapter<?>> getFieldSubsets() {
		return fieldSubsets;
	}

	public boolean isWholeRow() {
		return isWholeRow;
	}

	public boolean isAggregation() {
		return ((aggregation != null) && (aggregation.getRight() != null));
	}

	public QueryRanges getQueryRanges() {
		return queryRanges;
	}

	public DistributableQueryFilter getFilter() {
		return filter;
	}

	public Integer getLimit() {
		return limit;
	}

	public String[] getAdditionalAuthorizations() {
		return additionalAuthorizations;
	}
}
