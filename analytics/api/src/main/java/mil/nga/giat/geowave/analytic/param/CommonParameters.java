package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytic.model.IndexModelBuilder;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public class CommonParameters
{
	public enum Common
			implements
			ParameterEnum {
		DIMENSION_EXTRACT_CLASS(
				DimensionExtractor.class,
				"dde",
				"Dimension Extractor Class implements mil.nga.giat.geowave.analytics.extract.DimensionExtractor",
				true),
		DISTANCE_FUNCTION_CLASS(
				DistanceFn.class,
				"cdf",
				"Distance Function Class implements mil.nga.giat.geowave.analytics.distance.DistanceFn",
				true),
		INDEX_MODEL_BUILDER_CLASS(
				IndexModelBuilder.class,
				"cim",
				"Class implements mil.nga.giat.geowave.analytics.tools.model.IndexModelBuilder",
				true);

		private final Class<?> baseClass;

		private final Option[] options;

		Common(
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
