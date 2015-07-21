package mil.nga.giat.geowave.core.store.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.DataWriter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.log4j.Logger;

/*
 */
public class DataStoreUtils
{
	private final static Logger LOGGER = Logger.getLogger(DataStoreUtils.class);

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static final UniformVisibilityWriter DEFAULT_VISIBILITY = new UniformVisibilityWriter(
			new UnconstrainedVisibilityHandler());

	public static List<ByteArrayRange> constraintsToByteArrayRanges(
			final MultiDimensionalNumericData constraints,
			final NumericIndexStrategy indexStrategy ) {
		if ((constraints == null) || constraints.isEmpty()) {
			return new ArrayList<ByteArrayRange>(); // implies in negative and
			// positive infinity
		}
		else {
			return indexStrategy.getQueryRanges(constraints);
		}
	}

	public static List<ByteArrayRange> constraintsToByteArrayRanges(
			final MultiDimensionalNumericData constraints,
			final NumericIndexStrategy indexStrategy,
			final int maxRanges ) {
		if ((constraints == null) || constraints.isEmpty()) {
			return new ArrayList<ByteArrayRange>(); // implies in negative and
			// positive infinity
		}
		else {
			return indexStrategy.getQueryRanges(
					constraints,
					maxRanges);
		}
	}

	public static boolean isAuthorized(
			byte[] visibility,
			String... authorizations ) {
		if (visibility.length == 0) return true;
		final VisibilityExpression expr = new VisibilityExpressionParser().parse(visibility);
		return expr.ok(authorizations);
	}

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName : tableNamespace + "_" + unqualifiedTableName;
	}

	public static <T> List<EntryRow> entryToRows(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final T entry,
			final IngestCallback<T> ingestCallback,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final DataStoreEntryInfo ingestInfo = getIngestInfo(
				dataWriter,
				index,
				entry,
				customFieldVisibilityWriter);
		ingestCallback.entryIngested(
				ingestInfo,
				entry);
		return buildRows(
				dataWriter.getAdapterId().getBytes(),
				entry,
				ingestInfo);
	}

	protected static IndexedAdapterPersistenceEncoding getEncoding(
			final CommonIndexModel model,
			final DataAdapter<?> dataAdapter,
			final EntryRow row ) {
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		for (final FieldInfo column : row.info.getFieldInfo()) {
			final FieldReader<? extends CommonIndexValue> reader = model.getReader(column.getDataValue().getId());
			if (reader == null) {
				extendedData.addValue(column.getDataValue());
			}
			else {
				commonData.addValue(column.getDataValue());
			}
		}
		return new IndexedAdapterPersistenceEncoding(
				new ByteArrayId(
						row.getTableRowId().getAdapterId()),
				new ByteArrayId(
						row.getTableRowId().getDataId()),
				new ByteArrayId(
						row.getTableRowId().getInsertionId()),
				row.getTableRowId().getNumberOfDuplicates(),
				commonData,
				extendedData);
	}

	private static <T> List<EntryRow> buildRows(
			final byte[] adapterId,
			final T entry,
			final DataStoreEntryInfo ingestInfo ) {
		final List<EntryRow> rows = new ArrayList<EntryRow>();
		final List<FieldInfo> fieldInfoList = ingestInfo.getFieldInfo();
		for (final ByteArrayId rowId : ingestInfo.getRowIds()) {
			rows.add(new EntryRow(
					rowId,
					entry,
					ingestInfo));
		}
		return rows;
	}

	/**
	 * 
	 * @param dataWriter
	 * @param index
	 * @param entry
	 * @return List of zero or more matches
	 */
	public static <T> List<ByteArrayId> getRowIds(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final T entry ) {
		final CommonIndexModel indexModel = index.getIndexModel();
		final AdapterPersistenceEncoding encodedData = dataWriter.encode(
				entry,
				indexModel);
		final List<ByteArrayId> insertionIds = encodedData.getInsertionIds(index);
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>(
				insertionIds.size());

		addToRowIds(
				rowIds,
				insertionIds,
				dataWriter.getDataId(
						entry).getBytes(),
				dataWriter.getAdapterId().getBytes(),
				encodedData.isDeduplicationEnabled());

		return rowIds;
	}

	private static <T> void addToRowIds(
			final List<ByteArrayId> rowIds,
			final List<ByteArrayId> insertionIds,
			final byte[] dataId,
			final byte[] adapterId,
			final boolean enableDeduplication ) {

		final int numberOfDuplicates = insertionIds.size() - 1;

		for (final ByteArrayId insertionId : insertionIds) {
			final byte[] indexId = insertionId.getBytes();
			// because the combination of the adapter ID and data ID
			// gaurantees uniqueness, we combine them in the row ID to
			// disambiguate index values that are the same, also adding
			// enough length values to be able to read the row ID again, we
			// lastly add a number of duplicates which can be useful as
			// metadata in our de-duplication
			// step
			rowIds.add(new ByteArrayId(
					new EntryRowID(
							indexId,
							dataId,
							adapterId,
							enableDeduplication ? numberOfDuplicates : -1).getRowId()));
		}
	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static <T> DataStoreEntryInfo getIngestInfo(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final CommonIndexModel indexModel = index.getIndexModel();

		final AdapterPersistenceEncoding encodedData = dataWriter.encode(
				entry,
				indexModel);
		final List<ByteArrayId> insertionIds = encodedData.getInsertionIds(index);
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>(
				insertionIds.size());
		final PersistentDataset extendedData = encodedData.getAdapterExtendedData();
		final PersistentDataset indexedData = encodedData.getCommonData();
		final List<PersistentValue> extendedValues = extendedData.getValues();
		final List<PersistentValue> commonValues = indexedData.getValues();

		final List<FieldInfo> fieldInfoList = new ArrayList<FieldInfo>();

		if (!insertionIds.isEmpty()) {
			addToRowIds(
					rowIds,
					insertionIds,
					dataWriter.getDataId(
							entry).getBytes(),
					dataWriter.getAdapterId().getBytes(),
					encodedData.isDeduplicationEnabled());

			for (final PersistentValue fieldValue : commonValues) {
				final FieldInfo<T> fieldInfo = getFieldInfo(
						indexModel,
						fieldValue,
						entry,
						customFieldVisibilityWriter);
				if (fieldInfo != null) {
					fieldInfoList.add(fieldInfo);
				}
			}
			for (final PersistentValue fieldValue : extendedValues) {
				if (fieldValue.getValue() != null) {
					final FieldInfo<T> fieldInfo = getFieldInfo(
							dataWriter,
							fieldValue,
							entry,
							customFieldVisibilityWriter);
					if (fieldInfo != null) {
						fieldInfoList.add(fieldInfo);
					}
				}
			}
			return new DataStoreEntryInfo(
					rowIds,
					fieldInfoList);
		}
		LOGGER.warn("Indexing failed to produce insertion ids; entry [" + dataWriter.getDataId(
				entry).getString() + "] not saved.");
		return new DataStoreEntryInfo(
				Collections.EMPTY_LIST,
				Collections.EMPTY_LIST);

	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	private static <T> FieldInfo<T> getFieldInfo(
			final DataWriter dataWriter,
			final PersistentValue<T> fieldValue,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final FieldWriter fieldWriter = dataWriter.getWriter(fieldValue.getId());
		final FieldVisibilityHandler<T, Object> customVisibilityHandler = customFieldVisibilityWriter.getFieldVisibilityHandler(fieldValue.getId());
		if (fieldWriter != null) {
			final Object value = fieldValue.getValue();
			return new FieldInfo<T>(
					fieldValue,
					fieldWriter.writeField(value),
					merge(
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
			LOGGER.warn("Data writer of class " + dataWriter.getClass() + " does not support field for " + fieldValue.getValue());
		}
		return null;
	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	private static <T> FieldInfo<T> getFieldInfo(
			final PersistentValue<T> fieldValue,
			final byte[] value,
			final byte[] visibility ) {
		return new FieldInfo<T>(
				fieldValue,
				value,
				visibility);
	}

	private static final byte[] BEG_AND_BYTE = "&".getBytes(StringUtils.UTF8_CHAR_SET);
	private static final byte[] END_AND_BYTE = ")".getBytes(StringUtils.UTF8_CHAR_SET);

	private static byte[] merge(
			final byte vis1[],
			final byte vis2[] ) {
		if ((vis1 == null) || (vis1.length == 0)) {
			return vis2;
		}
		else if ((vis2 == null) || (vis2.length == 0)) {
			return vis1;
		}

		final ByteBuffer buffer = ByteBuffer.allocate(vis1.length + 3 + vis2.length);
		buffer.putChar('(');
		buffer.put(vis1);
		buffer.putChar(')');
		buffer.put(BEG_AND_BYTE);
		buffer.put(vis2);
		buffer.put(END_AND_BYTE);
		return buffer.array();
	}

	private abstract static class VisibilityExpression
	{

		public abstract boolean ok(
				String[] auths );

		public VisibilityExpression and() {
			AndExpression exp = new AndExpression();
			exp.add(this);
			return exp;
		}

		public VisibilityExpression or() {
			OrExpression exp = new OrExpression();
			exp.add(this);
			return exp;
		}

		public abstract List<VisibilityExpression> children();

		public abstract VisibilityExpression add(
				VisibilityExpression expression );

	}

	public static enum NodeType {
		TERM,
		OR,
		AND,
	}

	private static class VisibilityExpressionParser
	{
		private int index = 0;
		private int parens = 0;

		public VisibilityExpressionParser() {}

		VisibilityExpression parse(
				byte[] expression ) {
			if (expression.length > 0) {
				VisibilityExpression expr = parse_(expression);
				if (expr == null) {
					badArgumentException(
							"operator or missing parens",
							new String(
									expression),
							index - 1);
				}
				if (parens != 0) {
					badArgumentException(
							"parenthesis mis-match",
							new String(
									expression),
							index - 1);
				}
				return expr;
			}
			return null;
		}

		VisibilityExpression processTerm(
				int start,
				int end,
				VisibilityExpression expr,
				byte[] expression ) {
			if (start != end) {
				if (expr != null) badArgumentException(
						"expression needs | or &",
						new String(
								expression),
						start);
				return new ChildExpression(
						new String(Arrays.copyOfRange(
								expression,
								start,
								end)));
			}
			if (expr == null) badArgumentException(
					"empty term",
					new String(
							Arrays.copyOfRange(
									expression,
									start,
									end)),
					start);
			return expr;
		}

		VisibilityExpression parse_(
				byte[] expression ) {
			VisibilityExpression result = null;
			VisibilityExpression expr = null;
			int termStart = index;
			while (index < expression.length) {
				switch (expression[index++]) {
					case '&': {
						expr = processTerm(
								termStart,
								index - 1,
								expr,
								expression);
						if (result != null) {
							if (!(result instanceof AndExpression)) badArgumentException(
									"cannot mix & and |",
									new String(
											expression),
									index - 1);
						}
						else {
							result = new AndExpression();
						}
						result.add(expr);
						expr = null;
						termStart = index;
						break;
					}
					case '|': {
						expr = processTerm(
								termStart,
								index - 1,
								expr,
								expression);
						if (result != null) {
							if (!(result instanceof OrExpression)) badArgumentException(
									"cannot mix | and &",
									new String(
											expression),
									index - 1);
						}
						else {
							result = new OrExpression();
						}
						result.add(expr);
						expr = null;
						termStart = index;
						break;
					}
					case '(': {
						parens++;
						if (termStart != index - 1 || expr != null) badArgumentException(
								"expression needs & or |",
								new String(
										expression),
								index - 1);
						expr = parse_(expression);
						termStart = index;
						break;
					}
					case ')': {
						parens--;
						VisibilityExpression child = processTerm(
								termStart,
								index - 1,
								expr,
								expression);
						if (child == null && result == null) badArgumentException(
								"empty expression not allowed",
								new String(
										expression),
								index);
						if (result == null) return child;
						result.add(child);
						return result;
					}
				}
			}
			final VisibilityExpression child = processTerm(
					termStart,
					index,
					expr,
					expression);
			if (result != null)
				result.add(child);
			else
				result = child;
			if (!(result instanceof ChildExpression)) if (result.children().size() < 2) badArgumentException(
					"missing term",
					new String(
							expression),
					index);
			return result;
		}
	}

	public abstract static class CompositeExpression extends
			VisibilityExpression
	{
		protected final List<VisibilityExpression> expressions = new ArrayList<VisibilityExpression>();

		public VisibilityExpression add(
				VisibilityExpression expression ) {
			if (expression.getClass().equals(
					this.getClass())) {
				for (VisibilityExpression child : expression.children())
					this.add(child);
			}
			else
				expressions.add(expression);
			return this;
		}
	}

	public static class ChildExpression extends
			VisibilityExpression
	{
		private final String value;

		public ChildExpression(
				String value ) {
			super();
			this.value = value;
		}

		public boolean ok(
				String[] auths ) {
			for (String auth : auths)
				if (value.equals(auth)) return true;
			return false;
		}

		public List<VisibilityExpression> children() {
			return Collections.emptyList();
		}

		@Override
		public VisibilityExpression add(
				VisibilityExpression expression ) {
			return this;
		}
	}

	public static class AndExpression extends
			CompositeExpression
	{

		public List<VisibilityExpression> children() {
			return expressions;
		}

		public boolean ok(
				String[] auth ) {
			for (VisibilityExpression expression : expressions) {
				if (!expression.ok(auth)) return false;
			}
			return true;
		}

		public VisibilityExpression and(
				VisibilityExpression expression ) {
			return this;
		}
	}

	public static class OrExpression extends
			CompositeExpression
	{

		public boolean ok(
				String[] auths ) {
			for (VisibilityExpression expression : expressions) {
				if (expression.ok(auths)) return true;
			}
			return false;
		}

		public List<VisibilityExpression> children() {
			return expressions;
		}

		public VisibilityExpression or(
				VisibilityExpression expression ) {
			return this;
		}

	}

	private static final void badArgumentException(
			String msg,
			String expression,
			int place ) {
		throw new IllegalArgumentException(
				msg + " for " + expression + " at " + place);
	}
}
