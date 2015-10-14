package mil.nga.giat.geowave.core.store.index.temporal;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

public class FilterableDateRangeConstraint extends
		TemporalQueryConstraint
{

	public FilterableDateRangeConstraint(
			final ByteArrayId fieldId,
			final Date start,
			final Date end ) {
		super(
				fieldId,
				start,
				end);
	}

	public FilterableDateRangeConstraint(
			final ByteArrayId fieldId,
			final Date start,
			final Date end,
			final boolean rangeInclusive ) {
		super(
				fieldId,
				start,
				end,
				rangeInclusive);
	}

	@Override
	public DistributableQueryFilter getFilter() {
		return new DateRangeFilter(
				fieldId,
				start,
				end,
				rangeInclusive);
	}

	@Override
	public List<ByteArrayRange> getRange() {
		return Collections.singletonList(new ByteArrayRange(
				new ByteArrayId(
						TemporalIndexStrategy.toIndexByte(start)),
				new ByteArrayId(
						TemporalIndexStrategy.toIndexByte(end))));
	}

}
