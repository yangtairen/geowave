package mil.nga.giat.geowave.analytic.param;

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

		private final ParameterHelper<?> helper;

		private Input(
				final Class baseClass,
				final String name,
				final String description,
				final boolean hasArg ) {
			helper = new BasicParameterHelper(
					this,
					baseClass,
					name,
					description,
					hasArg);
		}

		@Override
		public Enum<?> self() {
			return this;
		}

		@Override
		public ParameterHelper<?> getHelper() {
			return helper;
		}
	}
}
