package mil.nga.giat.geowave.core.store.callback;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyValue;

public class IngestCallbackList<T, R extends GeoWaveKeyValue> implements
		IngestCallback<T, R>,
		Flushable,
		Closeable
{
	private final List<IngestCallback<T,R>> callbacks;

	public IngestCallbackList(
			final List<IngestCallback<T,R>> callbacks ) {
		this.callbacks = callbacks;
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final IngestCallback<T> callback : callbacks) {
			callback.entryIngested(
					entryInfo,
					entry);
		}
	}

	@Override
	public void close()
			throws IOException {
		for (final IngestCallback<T> callback : callbacks) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
	}

	@Override
	public void flush()
			throws IOException {
		for (final IngestCallback<T> callback : callbacks) {
			if (callback instanceof Flushable) {
				((Flushable) callback).flush();
			}
		}
	}

}
