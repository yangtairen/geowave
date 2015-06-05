package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public class JumpParameters
{
	public enum Jump
			implements
			ParameterEnum {
		RANGE_OF_CENTROIDS(
				NumericRange.class,
				"jrc",
				"Comma-separated range of centroids (e.g. 2,100)",
				true),
		KPLUSPLUS_MIN(
				Integer.class,
				"jkp",
				"The minimum k when K means ++ takes over sampling.",
				true),
		COUNT_OF_CENTROIDS(
				Integer.class,
				"jcc",
				"Set the count of centroids for one run of kmeans.",
				true);

		private final Class<?> baseClass;
		private final Option[] options;

		Jump(
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
