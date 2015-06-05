package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.analytic.extract.CentroidExtractor;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public class CentroidParameters
{
	public enum Centroid
			implements
			ParameterEnum {
		INDEX_ID(
				String.class,
				"cid",
				"Index Identifier for Centroids",
				true),
		DATA_TYPE_ID(
				String.class,
				"cdt",
				"Data Type ID for a centroid item",
				true),
		DATA_NAMESPACE_URI(
				String.class,
				"cns",
				"Data Type Namespace for centroid item",
				true),
		CONXVERGANCE_TOLERANCE(
				Double.class,
				"cct",
				"The alpha parameter measure the minimum covergence to reach before ",
				true),
		DISTORTION_TABLE_NAME(
				String.class,
				"cdt",
				"The name of the Accumulo holding the information distortion for each batch of K-Means",
				true),
		EXTRACTOR_CLASS(
				CentroidExtractor.class,
				"cce",
				"Centroid Exractor Class implements mil.nga.giat.geowave.analytics.extract.CentroidExtractor",
				true),
		WRAPPER_FACTORY_CLASS(
				AnalyticItemWrapperFactory.class,
				"cfc",
				"A factory class that implements mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory",
				true),
		ZOOM_LEVEL(
				Integer.class,
				"czl",
				"Zoom Level Number",
				true);

		private final Class<?> baseClass;
		private final Option[] options;

		Centroid(
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

		@Override
		public Enum<?> self() {
			return this;
		}
	}

}
