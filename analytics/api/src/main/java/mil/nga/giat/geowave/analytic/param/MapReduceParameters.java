package mil.nga.giat.geowave.analytic.param;

import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public class MapReduceParameters
{

	public enum MRConfig
			implements
			ParameterEnum {
		CONFIG_FILE(
				String.class,
				"conf",
				"MapReduce Configuration",
				true),
		HDFS_HOST_PORT(
				String.class,
				"hdfs",
				"HDFS hostname and port in the format hostname:port",
				true),
		HDFS_BASE_DIR(
				String.class,
				"hdfsbase",
				"Fully qualified path to the base directory in hdfs",
				true),
		YARN_RESOURCE_MANAGER(
				String.class,
				"resourceman",
				"Yarn resource manager hostname and port in the format hostname:port",
				true),
		JOBTRACKER_HOST_PORT(
				Integer.class,
				"jobtracker",
				"Hadoop job tracker hostname and port in the format hostname:port",
				true);

		private final Class<?> baseClass;
		private final Option[] options;

		MRConfig(
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

	public static final void fillOptions(
			final Set<Option> options ) {
		PropertyManagement.fillOptions(
				options,
				new ParameterEnum[] {
					MRConfig.CONFIG_FILE,
					MRConfig.HDFS_BASE_DIR,
					MRConfig.HDFS_HOST_PORT,
					MRConfig.JOBTRACKER_HOST_PORT,
					MRConfig.YARN_RESOURCE_MANAGER
				});
	}
}
