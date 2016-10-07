package mil.nga.giat.geowave.core.store.base;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.store.DataStoreOptions;

public class BaseDataStoreOptions implements
		DataStoreOptions
{
	@Parameter(names = "--persistAdapter", hidden = true, arity = 1)
	protected boolean persistAdapter = true;

	@Parameter(names = "--persistIndex", hidden = true, arity = 1)
	protected boolean persistIndex = true;

	@Parameter(names = "--persistDataStatistics", hidden = true, arity = 1)
	protected boolean persistDataStatistics = true;

	@Parameter(names = "--useAltIndex", hidden = true, arity = 1)
	protected boolean useAltIndex = false;

	@Parameter(names = "--createTable", hidden = true, arity = 1)
	protected boolean createTable = true;

	@Override
	public boolean isPersistAdapter() {
		return persistAdapter;
	}

	@Override
	public boolean isPersistIndex() {
		return persistIndex;
	}

	@Override
	public boolean isPersistDataStatistics() {
		return persistDataStatistics;
	}

	@Override
	public boolean isUseAltIndex() {
		return useAltIndex;
	}

	@Override
	public boolean isCreateTable() {
		return createTable;
	}

	public void setPersistAdapter(
			final boolean persistAdapter ) {
		this.persistAdapter = persistAdapter;
	}

	public void setPersistIndex(
			final boolean persistIndex ) {
		this.persistIndex = persistIndex;
	}

	public void setPersistDataStatistics(
			final boolean persistDataStatistics ) {
		this.persistDataStatistics = persistDataStatistics;
	}

	public void setUseAltIndex(
			final boolean useAltIndex ) {
		this.useAltIndex = useAltIndex;
	}

	public void setCreateTable(
			final boolean createTable ) {
		this.createTable = createTable;
	}

}
