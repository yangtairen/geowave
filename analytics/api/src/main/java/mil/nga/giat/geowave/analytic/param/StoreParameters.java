package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.core.store.DataStore;

public class StoreParameters
{
	public enum StoreParam implements
		ParameterEnum {
			DATA_STORE(new DataStoreParameterHelper());
		private final ParameterHelper<?> helper;

		private StoreParam(
				final ParameterHelper<?> helper ) {
			this.helper = helper;
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
