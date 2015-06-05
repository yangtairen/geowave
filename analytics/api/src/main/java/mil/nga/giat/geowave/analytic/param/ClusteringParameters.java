package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public class ClusteringParameters
{

	public enum Clustering
			implements
			ParameterEnum {
		MAX_REDUCER_COUNT(
				Integer.class,
				"crc",
				"Maximum Clustering Reducer Count",
				true),
		RETAIN_GROUP_ASSIGNMENTS(
				Boolean.class,
				"ga",
				"Retain Group assignments during execution",
				false),
		MAX_ITERATIONS(
				Integer.class,
				"cmi",
				"Maximum number of iterations when finding optimal clusters",
				true),
		CONVERGANCE_TOLERANCE(
				Double.class,
				"cct",
				"Convergence Tolerance",
				true),
		DISTANCE_THRESHOLDS(
				String.class,
				"dt",
				"Comma separated list of distance thresholds, per dimension",
				true),
		GEOMETRIC_DISTANCE_UNIT(
				String.class,
				"du",
				"Geometric distance unit (m=meters,km=kilometers, see symbols for javax.units.BaseUnit)",
				true),
		ZOOM_LEVELS(
				Integer.class,
				"zl",
				"Number of Zoom Levels to Process",
				true);

		private final Class<?> baseClass;
		private final Option[] options;

		Clustering(
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
