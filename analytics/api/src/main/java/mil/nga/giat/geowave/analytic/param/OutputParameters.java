package mil.nga.giat.geowave.analytic.param;

import org.apache.hadoop.fs.Path;

public class OutputParameters
{
	public enum Output
			implements
			ParameterEnum<Object> {
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
		private final ParameterHelper<Object> helper;

		private Output(
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
		public ParameterHelper<Object> getHelper() {
			return helper;
		}
	}
}
