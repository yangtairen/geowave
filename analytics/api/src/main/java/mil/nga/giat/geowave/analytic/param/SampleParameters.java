package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.analytic.sample.SampleProbabilityFn;
import mil.nga.giat.geowave.analytic.sample.function.SamplingRankFunction;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public class SampleParameters
{
	public enum Sample
			implements
			ParameterEnum {
		SAMPLE_SIZE(
				Integer.class,
				"sss",
				"Sample Size",
				true),
		MIN_SAMPLE_SIZE(
				Integer.class,
				"sms",
				"Minimum Sample Size",
				true),
		MAX_SAMPLE_SIZE(
				Integer.class,
				"sxs",
				"Max Sample Size",
				true),
		DATA_TYPE_ID(
				String.class,
				"sdt",
				"Sample Data Type Id",
				true),
		INDEX_ID(
				String.class,
				"sdt",
				"Sample Index Type Id",
				true),
		SAMPLE_ITERATIONS(
				Integer.class,
				"ssi",
				"Minimum number of sample iterations",
				true),
		PROBABILITY_FUNCTION(
				SampleProbabilityFn.class,
				"spf",
				"The PDF determines the probability for samping an item. Used by specific sample rank functions, such as CentroidDistanceBasedSamplingRankFunction.",
				true),
		SAMPLE_RANK_FUNCTION(
				SamplingRankFunction.class,
				"srf",
				"The rank function used when sampling the first N highest rank items.",
				true);

		private final Class<?> baseClass;
		private final Option[] options;

		Sample(
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
