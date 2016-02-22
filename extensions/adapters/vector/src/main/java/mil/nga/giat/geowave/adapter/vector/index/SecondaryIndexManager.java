package mil.nga.giat.geowave.adapter.vector.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import mil.nga.giat.geowave.adapter.vector.plugin.QueryIssuer;
import mil.nga.giat.geowave.adapter.vector.query.cql.PropertyConstraintSet;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.StatsManager;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.FieldIdStatisticVisibility;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.numeric.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.index.numeric.NumericQueryConstraint;
import mil.nga.giat.geowave.core.store.index.temporal.TemporalIndexStrategy;
import mil.nga.giat.geowave.core.store.index.text.TextIndexStrategy;
import mil.nga.giat.geowave.core.store.index.text.TextQueryConstraint;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

public class SecondaryIndexManager implements
		Persistable
{
	private final List<SecondaryIndex<SimpleFeature>> supportedSecondaryIndices = new ArrayList<>();
	final List<ByteArrayId> numericFields = new ArrayList<>();
	final List<ByteArrayId> textFields = new ArrayList<>();
	final List<ByteArrayId> temporalFields = new ArrayList<>();

	public SecondaryIndexManager(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType sft,
			final StatsManager statsManager ) {
		initialize(
				dataAdapter,
				sft,
				statsManager);
	}

	private void initialize(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType sft,
			final StatsManager statsManager ) {

		final List<DataStatistics<SimpleFeature>> secondaryIndexStatistics = new ArrayList<>();

		for (final AttributeDescriptor desc : sft.getAttributeDescriptors()) {
			final Map<Object, Object> userData = desc.getUserData();
			final String attributeName = desc.getLocalName();
			final ByteArrayId fieldId = new ByteArrayId(
					attributeName);
			if (userData.containsKey(NumericSecondaryIndexConfiguration.INDEX_KEY) && userData.get(
					NumericSecondaryIndexConfiguration.INDEX_KEY).equals(
					Boolean.TRUE)) {
				numericFields.add(fieldId);
			}
			else if (userData.containsKey(TextSecondaryIndexConfiguration.INDEX_KEY) && userData.get(
					TextSecondaryIndexConfiguration.INDEX_KEY).equals(
					Boolean.TRUE)) {
				textFields.add(fieldId);
			}
			else if (userData.containsKey(TemporalSecondaryIndexConfiguration.INDEX_KEY) && userData.get(
					TemporalSecondaryIndexConfiguration.INDEX_KEY).equals(
					Boolean.TRUE)) {
				temporalFields.add(fieldId);
			}
		}

		if (numericFields.size() > 0) {
			final List<DataStatistics<SimpleFeature>> numericStatistics = new ArrayList<>();
			for (final ByteArrayId numericField : numericFields) {
				numericStatistics.add(new FeatureNumericHistogramStatistics(
						dataAdapter.getAdapterId(),
						numericField.getString()));
			}
			supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
					new NumericIndexStrategy(),
					numericFields.toArray(new ByteArrayId[numericFields.size()]),
					numericStatistics));
			secondaryIndexStatistics.addAll(numericStatistics);
		}
		if (textFields.size() > 0) {
			final List<DataStatistics<SimpleFeature>> textStatistics = new ArrayList<>();
			for (final ByteArrayId textField : textFields) {
				textStatistics.add(new FeatureHyperLogLogStatistics(
						dataAdapter.getAdapterId(),
						textField.getString(),
						16));
			}
			supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
					new TextIndexStrategy(),
					textFields.toArray(new ByteArrayId[textFields.size()]),
					textStatistics));
			secondaryIndexStatistics.addAll(textStatistics);
		}
		if (temporalFields.size() > 0) {
			final List<DataStatistics<SimpleFeature>> temporalStatistics = new ArrayList<>();
			for (final ByteArrayId temporalField : temporalFields) {
				temporalStatistics.add(new FeatureNumericHistogramStatistics(
						dataAdapter.getAdapterId(),
						temporalField.getString()));
			}
			supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
					new TemporalIndexStrategy(),
					temporalFields.toArray(new ByteArrayId[temporalFields.size()]),
					temporalStatistics));
			secondaryIndexStatistics.addAll(temporalStatistics);
		}

		for (final DataStatistics<SimpleFeature> secondaryIndexStatistic : secondaryIndexStatistics) {
			statsManager.addStats(
					secondaryIndexStatistic,
					new FieldIdStatisticVisibility<SimpleFeature>(
							secondaryIndexStatistic.getStatisticsId()));
		}
	}

	public CloseableIterator<SimpleFeature> query(
			final QueryIssuer queryIssuer,
			final ByteArrayId dataAdapterId,
			final DataStore dataStore,
			final Query query,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
			final PrimaryIndex index,
			final PropertyConstraintSet constraintSet,
			final String... visibility )
			throws IOException {
		final List<CloseableIterator<ByteArrayId>> setsOfSecondaryIndexResults = new ArrayList<CloseableIterator<ByteArrayId>>();
		if (!constraintSet.isEmpty()) {
			for (SecondaryIndex<SimpleFeature> secondaryIndex : supportedSecondaryIndices) {
				for (ByteArrayId fieldId : secondaryIndex.getFieldIDs()) {
					final FilterableConstraints constraints = constraintSet.getConstraintsById(fieldId);
					if (constraints != null) {
						final FeatureNumericHistogramStatistics hist = (FeatureNumericHistogramStatistics) stats.get(FeatureNumericHistogramStatistics.composeId(fieldId.getString()));
						// final FeatureHyperLogLogStatistics loglog =
						// (FeatureHyperLogLogStatistics)
						// stats.get(FeatureHyperLogLogStatistics.composeId(fieldId.getString()));
						if (numericFields.contains(fieldId) && hist != null) {
							final NumericQueryConstraint numConstraints = (NumericQueryConstraint) constraints;
							final double populationPercent = hist.percentPopulationOverRange(
									numConstraints.getMinValue(),
									numConstraints.getMaxValue());
							final double count = Math.ceil(hist.totalSampleSize() * populationPercent);
							// bogus number...for now
							if (count > 20000 || populationPercent > 0.05) {
								continue;
							}
							else if (textFields.contains(fieldId)) {
								// well..ideally we would use some histogram
								// but we can always attempt to get the first
								// 10000 and
								// see how things go
							}
							else {
								continue;
							}
							setsOfSecondaryIndexResults.add(secondaryIndexDataStore.query(
									secondaryIndex,
									secondaryIndex.getIndexStrategy().getQueryRanges(
											constraints),
									Collections.singletonList(constraints.getFilter()),
									index.getId(),
									visibility));
						}
					}
				}
			}
		}
		if (setsOfSecondaryIndexResults.size() > 0) {
			return resolveSecondaryIndexQuery(
					setsOfSecondaryIndexResults,
					dataAdapterId,
					dataStore,
					queryIssuer,
					query,
					index,
					10000); // see comment on the method about this value
		}
		return queryIssuer.query(
				index,
				query);
	}

	/**
	 * Perform set intersection and then, for each unique primary index
	 * identifier, lookup the feature. If the maximum cutoff is met, then issue
	 * the query to the primary index.
	 * 
	 * Ideally, the maxCutoff would be some percentage of the the expected
	 * results number of entries inspected if the query were to be run directly
	 * on the primary index (i.e. those entries constrained only by the
	 * multi-dimension numeric constraints, not the additional filtered
	 * constraints).
	 * 
	 * The intersection (common IDs) returned from the list of ID iterators is
	 * used to resolve primary index IDs.
	 * 
	 * This method does not let the list of ID iterators to exceed the max cutoff.
	 * Those iterators are deemed not supportive and not considered in the ID intersection. 
	 * 
	 * Ideally, the calling method will not provide ID iterators for those secondary index queries
	 * that cannot reduce the number of primary index identifiers below the cut-off.  
	 * 
	 * @param idsIterator
	 * @param dataAdapterId
	 * @param dataStore
	 * @param queryIssuer
	 * @param query
	 * @param index
	 * @param maxCutoff
	 *            -- maximum records to lookup from the primary index
	 * @return
	 * @throws IOException
	 */
	private CloseableIterator<SimpleFeature> resolveSecondaryIndexQuery(
			final List<CloseableIterator<ByteArrayId>> idsIterator,
			final ByteArrayId dataAdapterId,
			final DataStore dataStore,
			final QueryIssuer queryIssuer,
			final Query query,
			final PrimaryIndex index,
			final int maxCutoff )
			throws IOException {
		final Map<ByteArrayId, BitSet> primaryIds = new HashMap<ByteArrayId, BitSet>();
		boolean primePhase = true;
		int iteratorMaskId = 0;
		final BitSet validIteratorIds = new BitSet();
		for (CloseableIterator<ByteArrayId> iterator : idsIterator) {
			int idCount = 0;
			while (iterator.hasNext()) {
				idCount++;
				if (idCount > maxCutoff) {
					// do not continue, exceeded maximum allowed
					break;
				}
				final ByteArrayId id = iterator.next();
				BitSet mask = primaryIds.get(id);
				if (mask == null) {
					if (!primePhase) continue; // optimization...do not need to
												// add a set that is not already
												// added.
					mask = new BitSet();
					primaryIds.put(
							id,
							mask);
				}
				mask.set(iteratorMaskId);
			}
			iterator.close();
			if (idCount > maxCutoff) {
				// finished this set of ids without issue, so this is a 'valid'
				// set
				validIteratorIds.set(iteratorMaskId);
			}
			else {
				// primePhase is over as long as we have not run through an invalid set of ids (i.e. too many)
				primePhase = false;
			}
			iteratorMaskId++;
		}
		if (validIteratorIds.cardinality() > 0) {
			final Iterator<Entry<ByteArrayId, BitSet>> it = primaryIds.entrySet().iterator();
			while (it.hasNext()) {
				final Map.Entry<ByteArrayId, BitSet> entry = it.next();
				final BitSet set = entry.getValue();
				final int initialSet = set.cardinality();
				set.and(validIteratorIds);
				// if the number of bits set is not the same, then
				// this identifier did not have agreement across
				// valid indices
				if (initialSet != set.cardinality()) it.remove();
			}
		}
		else {
			primaryIds.clear();
		}
		if (primaryIds.size() > maxCutoff) {
			return queryIssuer.query(
					index,
					query);
		}
		final QueryOptions options = new QueryOptions(
				index);
		return new CloseableIterator<SimpleFeature>() {

			final Iterator<ByteArrayId> it = primaryIds.keySet().iterator();
			SimpleFeature nextFeature = null;

			@Override
			public boolean hasNext() {
				while (nextFeature == null && it.hasNext()) {
					// Recall that 'deletions' from seconary indices can be
					// behind the primary
					// thus containing bad references
					try (CloseableIterator<SimpleFeature> sfIt = dataStore.query(
							options,
							new DataIdQuery(
									dataAdapterId,
									it.next()))) {
						if (it.hasNext()) nextFeature = sfIt.next();
					}
					catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				return nextFeature != null;
			}

			@Override
			public SimpleFeature next() {
				SimpleFeature r = nextFeature;
				nextFeature = null;
				return r;

			}

			@Override
			public void close()
					throws IOException {}

		};
	}

	public List<SecondaryIndex<SimpleFeature>> getSupportedSecondaryIndices() {
		return supportedSecondaryIndices;
	}

	@Override
	public byte[] toBinary() {
		final List<Persistable> persistables = new ArrayList<Persistable>();
		for (final SecondaryIndex<SimpleFeature> secondaryIndex : supportedSecondaryIndices) {
			persistables.add(secondaryIndex);
		}
		return PersistenceUtils.toBinary(persistables);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromBinary(
			byte[] bytes ) {
		final List<Persistable> persistables = PersistenceUtils.fromBinary(bytes);
		for (final Persistable persistable : persistables) {
			supportedSecondaryIndices.add((SecondaryIndex<SimpleFeature>) persistable);
		}
	}

}
