package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public class ExtractParameters
{
	public enum Extract
			implements
			ParameterEnum {
		OUTPUT_DATA_TYPE_ID(
				String.class,
				"eot",
				"Output Data Type ID",
				true),
		DATA_NAMESPACE_URI(
				String.class,
				"ens",
				"Output Data Namespace URI",
				true),
		REDUCER_COUNT(
				Integer.class,
				"erc",
				"Number of Reducers For initial data extraction and de-duplication",
				true),
		DIMENSION_EXTRACT_CLASS(
				DimensionExtractor.class,
				"ede",
				"Class to extract dimensions into a simple feature output",
				true),
		QUERY(
				DistributableQuery.class,
				"eq",
				"Query",
				true),
		MAX_INPUT_SPLIT(
				Integer.class,
				"emx",
				"Maximum input split size",
				true),
		MIN_INPUT_SPLIT(
				Integer.class,
				"emn",
				"Minimum input split size",
				true),
		INDEX_ID(
				String.class,
				"ei",
				"Extract from a specific index",
				true),
		GROUP_ID(
				String.class,
				"eg",
				"Group ID assigned to extracted data",
				true),
		ADAPTER_ID(
				String.class,
				"eit",
				"Input Data Type ID",
				true);

		private final Class<?> baseClass;
		private final Option[] options;

		Extract(
				final Class<?> baseClass,
				final String name,
				final String description,
				final boolean hasArg ) {
			this.baseClass = baseClass;
			options = new Option[] {
				PropertyManagement.newOption(
						this,
						name,
						description,
						hasArg)
			};
		}

		@Override
		public Class<?> getBaseClass() {
			return baseClass;
		}

		@Override
		public Enum<?> self() {
			return this;
		}

		@Override
		public Option[] getOptions() {
			return options;
		}

		@Override
		public void setParameter(
				final Configuration jobConfig,
				final Class<?> jobScope,
				final PropertyManagement propertyValues ) {
			RunnerUtils.setParameter(
					jobConfig,
					jobScope,
					propertyValues,
					this);
		}
	}
}
