package mil.nga.giat.geowave.datastore.accumulo;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;

/**
 * This is a basic wrapper around the Accumulo batch writer so that write
 * operations will use an interface that can be implemented differently for
 * different purposes. For example, a bulk ingest can be performed by replacing
 * this implementation within a custom implementation of AccumuloOperations.
 */
public class BatchWriterWrapper implements
		Writer
{
	private final static Logger LOGGER = Logger.getLogger(
			BatchWriterWrapper.class);
	private org.apache.accumulo.core.client.BatchWriter batchWriter;

	public BatchWriterWrapper(
			final org.apache.accumulo.core.client.BatchWriter batchWriter ) {
		this.batchWriter = batchWriter;
	}

	public org.apache.accumulo.core.client.BatchWriter getBatchWriter() {
		return batchWriter;
	}

	public void setBatchWriter(
			final org.apache.accumulo.core.client.BatchWriter batchWriter ) {
		this.batchWriter = batchWriter;
	}

	public void write(
			final Iterable<Mutation> mutations ) {
		try {
			batchWriter.addMutations(
					mutations);
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to close batch writer",
					e);
		}
	}

	public void write(
			final Mutation mutation ) {
		try {
			batchWriter.addMutation(
					mutation);
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to write batch writer",
					e);
		}
	}

	@Override
	public void close() {
		try {
			batchWriter.close();
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to close batch writer",
					e);
		}
	}

	@Override
	public void flush() {
		try {
			batchWriter.flush();
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to flush batch writer",
					e);
		}
	}

	@Override
	public void write(
			GeoWaveRow[] rows ) {
		for (GeoWaveRow row : rows) {
			write(
					row);
		}
	}

	@Override
	public void write(
			GeoWaveRow row ) {
		write(rowToMutation(row));
	}

	private static Mutation rowToMutation(
			GeoWaveRow row ) {
		final Mutation mutation = new Mutation(GeoWaveKey.getCompositeId(row));
		for (final GeoWaveValue value : row.getFieldValues()) {
			if (value.getVisibility() != null && value.getVisibility().length > 0) {
				mutation.put(
						new Text(row.getAdapterId()),
						new Text(
								value.getFieldMask()),
						new ColumnVisibility(
								value.getVisibility()),
						new Value(
								value.getValue()));
			}
			else {
				mutation.put(
						new Text(
								row.getAdapterId()),
						new Text(
								value.getFieldMask()),
						new Value(
								value.getValue()));
			}
		}

		return mutation;
	}

}
