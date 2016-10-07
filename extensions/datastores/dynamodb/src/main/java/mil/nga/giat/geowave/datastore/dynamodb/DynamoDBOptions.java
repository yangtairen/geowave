package mil.nga.giat.geowave.datastore.dynamodb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.base.BaseDataStoreOptions;

public class DynamoDBOptions extends
		StoreFactoryOptions implements
		DataStoreOptions
{
	@Parameter(names = "--endpoint")
	protected String endpoint;

	@ParametersDelegate
	protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions();

	public void setEndpoint(
			String endpoint ) {
		this.endpoint = endpoint;
	}

	public String getEndpoint() {
		return endpoint;
	}

	@Override
	public boolean isPersistAdapter() {
		return baseOptions.isPersistAdapter();
	}

	@Override
	public boolean isPersistIndex() {
		return baseOptions.isPersistIndex();
	}

	@Override
	public boolean isPersistDataStatistics() {
		return baseOptions.isPersistDataStatistics();
	}

	@Override
	public boolean isUseAltIndex() {
		return baseOptions.isUseAltIndex();
	}

	@Override
	public boolean isCreateTable() {
		return baseOptions.isCreateTable();
	}

	public void setPersistAdapter(
			final boolean persistAdapter ) {
		baseOptions.setPersistAdapter(
				persistAdapter);
	}

	public void setPersistIndex(
			final boolean persistIndex ) {
		baseOptions.setPersistIndex(
				persistIndex);
	}

	public void setPersistDataStatistics(
			final boolean persistDataStatistics ) {
		baseOptions.setPersistDataStatistics(
				persistDataStatistics);
	}

	public void setUseAltIndex(
			final boolean useAltIndex ) {
		baseOptions.setUseAltIndex(
				useAltIndex);
	}

	public void setCreateTable(
			final boolean createTable ) {
		baseOptions.setCreateTable(
				createTable);
	}

}
