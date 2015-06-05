package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class InputParameters
{
	public enum Input
			implements
			ParameterEnum {
		INPUT_FORMAT(
				FormatConfiguration.class,
				"ifc",
				"Input Format Class",
				true),
		HDFS_INPUT_PATH(
				Path.class,
				"iip",
				"Input HDFS File Path",
				true);

		private final Class<?> baseClass;
		private final Option[] options;

		Input(
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
