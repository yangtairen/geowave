package mil.nga.giat.geowave.datastore.accumulo.metadata;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.metadata.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.metadata.MetadataWriter;

public class AccumuloMetadataWriter implements
		MetadataWriter
{
	private static final Logger LOGGER = LoggerFactory.getLogger(
			AccumuloMetadataWriter.class);
	private final BatchWriter writer;
	private final String persistenceTypeName;

	public AccumuloMetadataWriter(
			final BatchWriter writer,
			final String persistenceTypeName ) {
		this.writer = writer;
		this.persistenceTypeName = persistenceTypeName;
	}

	@Override
	public void close()
			throws Exception {
		try {
			writer.close();
		}
		catch (final MutationsRejectedException e) {
			LOGGER.warn(
					"Unable to close metadata writer",
					e);
		}
	}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {
		final Mutation mutation = new Mutation(
				new Text(
						metadata.getPrimaryId()));
		final Text cf = getSafeText(
				persistenceTypeName);
		final Text cq = metadata.getSecondaryId() != null ? new Text(
				metadata.getSecondaryId()) : new Text();
		final byte[] visibility = metadata.getVisibility();
		if (visibility != null) {
			mutation.put(
					cf,
					cq,
					new ColumnVisibility(
							visibility),
					new Value(
							metadata.getValue()));
		}
		else {
			mutation.put(
					cf,
					cq,
					new Value(
							metadata.getValue()));
		}
		try {
			writer.addMutation(
					mutation);
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to write metadata",
					e);
		}

	}

	private static Text getSafeText(
			final String text ) {
		if ((text != null) && !text.isEmpty()) {
			return new Text(
					text);
		}
		else {
			return new Text();
		}
	}

	@Override
	public void flush() {
		try {
			writer.flush();
		}
		catch (final MutationsRejectedException e) {
			LOGGER.warn(
					"Unable to flush metadata writer",
					e);
		}
	}

}
