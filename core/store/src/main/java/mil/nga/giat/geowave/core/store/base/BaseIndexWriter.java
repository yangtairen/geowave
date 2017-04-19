package mil.nga.giat.geowave.core.store.base;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.IntermediaryWriteEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.DataWriter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class BaseIndexWriter<T> implements
		IndexWriter<T>
{
	private final static Logger LOGGER = Logger.getLogger(
			BaseIndexWriter.class);

	protected final PrimaryIndex index;
	protected final DataStoreOperations operations;
	protected final DataStoreOptions options;
	protected final IngestCallback<T> callback;
	protected Writer writer;

	protected final WritableDataAdapter<T> adapter;
	protected final byte[] adapterId;
	final Closeable closable;

	public BaseIndexWriter(
			final WritableDataAdapter<T> adapter,
			final PrimaryIndex index,
			final DataStoreOperations operations,
			final DataStoreOptions options,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		this.operations = operations;
		this.options = options;
		this.index = index;
		this.callback = callback;
		this.adapter = adapter;
		this.adapterId = adapter.getAdapterId().getBytes();
		this.closable = closable;
	}

	@Override
	public PrimaryIndex[] getIndices() {
		return new PrimaryIndex[] {
			index
		};
	}

	@Override
	public InsertionIds write(
			final T entry ) {
		return write(
				entry,
				DataStoreUtils.UNCONSTRAINED_VISIBILITY);
	}

	@Override
	public InsertionIds write(
			final T entry,
			final VisibilityWriter<T> fieldVisibilityWriter ) {
		IntermediaryWriteEntryInfo entryInfo;
		synchronized (this) {

			ensureOpen();
			if (writer == null) {
				return new InsertionIds();
			}
			entryInfo = getEntryInfo(
					entry,
					fieldVisibilityWriter);
			if (entryInfo == null) {
				return new InsertionIds();
			}
			final GeoWaveRow[] rows = entryInfo.getRows();
			writer.write(
					rows);
			callback.entryIngested(
					entry,
					rows);
		}
		return entryInfo.getInsertionIds();
	}

	@Override
	public void close() {
		try {
			closable.close();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Cannot close callbacks",
					e);
		}
		// thread safe close
		closeInternal();
	}

	@Override
	public synchronized void flush() {
		// thread safe flush of the writers
		if (writer != null) {
			writer.flush();
		}
		if (this.callback instanceof Flushable) {
			try {
				((Flushable) callback).flush();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Cannot flush callbacks",
						e);
			}
		}
	}

	public IntermediaryWriteEntryInfo getEntryInfo(
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final CommonIndexModel indexModel = index.getIndexModel();

		final AdapterPersistenceEncoding encodedData = adapter.encode(
				entry,
				indexModel);
		final InsertionIds insertionIds = encodedData.getInsertionIds(
				index);
		final PersistentDataset extendedData = encodedData.getAdapterExtendedData();
		final PersistentDataset indexedData = encodedData.getCommonData();
		final List<PersistentValue> extendedValues = extendedData.getValues();
		final List<PersistentValue> commonValues = indexedData.getValues();

		List<FieldInfo<?>> fieldInfoList = new ArrayList<FieldInfo<?>>();

		final byte[] dataId = adapter.getDataId(
				entry).getBytes();
		final byte[] adapterId = adapter.getAdapterId().getBytes();
		if (!insertionIds.isEmpty()) {
			for (final PersistentValue fieldValue : commonValues) {
				final FieldInfo<?> fieldInfo = getFieldInfo(
						indexModel,
						fieldValue,
						entry,
						customFieldVisibilityWriter);
				if (fieldInfo != null) {
					fieldInfoList.add(
							fieldInfo);
				}
			}
			for (final PersistentValue<?> fieldValue : extendedValues) {
				if (fieldValue.getValue() != null) {
					final FieldInfo<?> fieldInfo = getFieldInfo(
							adapter,
							fieldValue,
							entry,
							customFieldVisibilityWriter);
					if (fieldInfo != null) {
						fieldInfoList.add(
								fieldInfo);
					}
				}
			}
		}
		else {
			LOGGER.warn(
					"Indexing failed to produce insertion ids; entry [" + adapter.getDataId(
							entry).getString() + "] not saved.");
		}

		fieldInfoList = DataStoreUtils.composeFlattenedFields(
				fieldInfoList,
				index.getIndexModel(),
				adapter);
		byte[] uniqueDataId;
		if ((adapter instanceof RowMergingDataAdapter) && (((RowMergingDataAdapter) adapter).getTransform() != null)) {
			uniqueDataId = DataStoreUtils.ensureUniqueId(
					dataId,
					false).getBytes();
		}
		else {
			uniqueDataId = dataId;
		}
		final IntermediaryWriteEntryInfo ingestInfo = new IntermediaryWriteEntryInfo(
				uniqueDataId,
				adapterId,
				insertionIds,
				fieldInfoList);
		verifyVisibility(
				customFieldVisibilityWriter,
				ingestInfo);

		return ingestInfo;
	}

	private void verifyVisibility(
			final VisibilityWriter customFieldVisibilityWriter,
			final IntermediaryWriteEntryInfo ingestInfo ) {
		if (customFieldVisibilityWriter != DataStoreUtils.UNCONSTRAINED_VISIBILITY) {
			for (final FieldInfo field : ingestInfo.getFieldInfo()) {
				if ((field.getVisibility() != null) && (field.getVisibility().length > 0)) {
					if (!operations.insureAuthorizations(
							null,
							StringUtils.stringFromBinary(
									field.getVisibility()))) {
						LOGGER.error(
								"Unable to set authorizations for ingested visibility '" + StringUtils.stringFromBinary(
										field.getVisibility()) + "'");
					}

				}
			}
		}
	}

	private static <T> FieldInfo<?> getFieldInfo(
			final DataWriter dataWriter,
			final PersistentValue<?> fieldValue,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final FieldWriter fieldWriter = dataWriter.getWriter(
				fieldValue.getId());
		final FieldVisibilityHandler<T, Object> customVisibilityHandler = customFieldVisibilityWriter
				.getFieldVisibilityHandler(
						fieldValue.getId());
		if (fieldWriter != null) {
			final Object value = fieldValue.getValue();
			return new FieldInfo(
					fieldValue,
					fieldWriter.writeField(
							value),
					DataStoreUtils.mergeVisibilities(
							customVisibilityHandler.getVisibility(
									entry,
									fieldValue.getId(),
									value),
							fieldWriter.getVisibility(
									entry,
									fieldValue.getId(),
									value)));
		}
		else if (fieldValue.getValue() != null) {
			LOGGER.warn(
					"Data writer of class " + dataWriter.getClass() + " does not support field for "
							+ fieldValue.getValue());
		}
		return null;
	}

	protected synchronized void closeInternal() {
		if (writer != null) {
			try {
				writer.close();
				writer = null;
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to close writer",
						e);
			}
		}
	}

	protected synchronized void ensureOpen() {
		if (writer == null) {
			try {
				writer = operations.createWriter(
						index.getId(),
						adapter.getAdapterId(),
						options,
						index.getIndexStrategy().getPartitionKeys());
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to open writer",
						e);
			}
		}
	}
}
