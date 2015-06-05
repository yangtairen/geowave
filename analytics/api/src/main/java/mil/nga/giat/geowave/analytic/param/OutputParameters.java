package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class OutputParameters
{
	public enum Output
			implements
			ParameterEnum {
		REDUCER_COUNT(
				Integer.class,
				"orc",
				"Number of Reducers For Output",
				true),
		OUTPUT_FORMAT(
				FormatConfiguration.class,
				"ofc",
				"Output Format Class",
				true),
		HDFS_OUTPUT_PATH(
				Path.class,
				"oop",
				"Output HDFS File Path",
				true);
		private final Class<?> baseClass;
		private final Option[] options;

		Output(
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
