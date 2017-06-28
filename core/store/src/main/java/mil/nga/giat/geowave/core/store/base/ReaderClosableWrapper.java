package mil.nga.giat.geowave.core.store.base;

import java.io.Closeable;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.operations.Reader;

class ReaderClosableWrapper implements
		Closeable
{
	private final static Logger LOGGER = Logger.getLogger(
			ReaderClosableWrapper.class);
	private final Reader reader;

	public ReaderClosableWrapper(
			final Reader reader ) {
		this.reader = reader;
	}

	@Override
	public void close() {
		try {
			reader.close();
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to close datastore reader",
					e);
		}
	}
}
