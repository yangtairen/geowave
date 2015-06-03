package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.DataStore;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class GlobalParameters
{
	public enum Global
			implements
			ParameterEnum {
		DATA_STORE(
				DataStoreCommandLineOptions.class),
		PARENT_BATCH_ID(
				String.class),
		CRS_ID(
				String.class),
		BATCH_ID(
				String.class);
		private final Class<?> baseClass;

		Global(
				final Class<?> baseClass ) {
			this.baseClass = baseClass;
		}

		@Override
		public Class<?> getBaseClass() {
			return baseClass;
		}

		@Override
		public Enum<?> self() {
			return this;
		}
	}

	public static final void fillOptions(
			final Set<Option> options,
			final Global[] params ) {
		if (contains(
				params,
				Global.DATA_STORE)) {
			final Options allOptions = new Options();
			DataStoreCommandLineOptions.applyOptions(allOptions);
			options.addAll(allOptions.getOptions());
		}
		if (contains(
				params,
				Global.BATCH_ID)) {
			options.add(PropertyManagement.newOption(
					Global.BATCH_ID,
					"b",
					"Batch ID",
					true));
		}
		if (contains(
				params,
				Global.PARENT_BATCH_ID)) {
			options.add(PropertyManagement.newOption(
					Global.PARENT_BATCH_ID,
					"pb",
					"Batch ID",
					true));
		}
		if (contains(
				params,
				Global.CRS_ID)) {
			options.add(PropertyManagement.newOption(
					Global.CRS_ID,
					"crs",
					"CRS ID",
					true));
		}
	}

	private static boolean contains(
			final Global[] params,
			final Global option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
