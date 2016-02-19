package mil.nga.giat.geowave.test.query;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.index.NumericSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TemporalSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TextSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexQueryManager;
import mil.nga.giat.geowave.core.store.index.numeric.NumericEqualsConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericGreaterThanConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericGreaterThanOrEqualToConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.index.numeric.NumericLessThanConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericLessThanOrEqualToConstraint;
import mil.nga.giat.geowave.core.store.index.temporal.TemporalIndexStrategy;
import mil.nga.giat.geowave.core.store.index.temporal.TemporalQueryConstraint;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

/**
 * Directly tests secondary indices by ingesting some data, ensuring the correct
 * # of entries in each secondary index table, and testing various constraints
 * 
 * @since 0.9.1
 */
public class SecondaryIndexingIT extends
		GeoWaveTestEnvironment
{
	private static final List<SimpleFeature> features = new ArrayList<>();
	private static DataStore store;
	private static PrimaryIndex primaryIndex;
	private static FeatureDataAdapter adapter;
	private static SecondaryIndexQueryManager secondaryIndexQueryManager;
	private static final com.vividsolutions.jts.geom.Geometry queryGeom = GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
			Location.HONOLULU.getPoint().getCoordinate(),
			Location.MONTREAL.getPoint().getCoordinate()));
	private static final ByteArrayId siblingsFieldId = new ByteArrayId(
			StringUtils.stringToBinary(Person.SIBLINGS.getValue()));
	private static final ByteArrayId birthDateFieldId = new ByteArrayId(
			StringUtils.stringToBinary(Person.BIRTHDATE.getValue()));

	/**
	 * Verifies the # of entries in each secondary index table
	 * 
	 * @throws TableNotFoundException
	 */
	@Test
	public void testIngest()
			throws TableNotFoundException {

		// secondary idx structure stores two vals per logical row
		final int ENTRIES_PER = 2;

		// numeric
		final int numNumericEntries = countNumberOfEntriesInIndexTable(NUMERIC_INDEX);
		final int expectedNumericEntries = 4 * ENTRIES_PER;
		Assert.assertTrue(
				"Expected " + expectedNumericEntries + " but was " + numNumericEntries,
				expectedNumericEntries == numNumericEntries);

		// text
		final int expectedTextEntries = 522; // sample data generates 522 unique
												// bi-grams, tri-grams,
												// quad-grams
		final int numTextEntries = countNumberOfEntriesInIndexTable(TEXT_INDEX);
		Assert.assertTrue(
				"Expected " + expectedTextEntries + " but was " + numTextEntries,
				expectedTextEntries == numTextEntries);

		// date
		final int numTemporalEntries = countNumberOfEntriesInIndexTable(TEMPORAL_INDEX);
		final int expectedTemporalEntries = 4 * ENTRIES_PER;
		Assert.assertTrue(
				"Expected " + expectedTemporalEntries + " but was " + numTemporalEntries,
				expectedTemporalEntries == numTemporalEntries);
	}

	@Test
	public void testGreaterThanOrEqualToConstraint()
			throws IOException {
		final List<FilterableConstraints> numericConstraints = new ArrayList<>();
		final Map<ByteArrayId, List<FilterableConstraints>> additionalConstraints = new HashMap<>();
		numericConstraints.add(new NumericGreaterThanOrEqualToConstraint(
				siblingsFieldId,
				1));
		additionalConstraints.put(
				siblingsFieldId,
				numericConstraints);
		for (final SecondaryIndex<?> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			int numMatches = 0;
			if (secondaryIndex.getIndexStrategy() instanceof NumericIndexStrategy) {
				try (final CloseableIterator<ByteArrayId> matches = secondaryIndexQueryManager.query(
						new SpatialQuery(
								queryGeom,
								additionalConstraints),
						secondaryIndex,
						primaryIndex)) {
					while (matches.hasNext()) {
						numMatches++;
						matches.next();
					}
				}
				Assert.assertTrue(
						"Expected 3 but was " + numMatches,
						numMatches == 3);
			}
		}
	}

	@Test
	public void testGreaterThanConstraint()
			throws IOException {
		final List<FilterableConstraints> numericConstraints = new ArrayList<>();
		final Map<ByteArrayId, List<FilterableConstraints>> additionalConstraints = new HashMap<>();
		numericConstraints.add(new NumericGreaterThanConstraint(
				siblingsFieldId,
				1));
		additionalConstraints.put(
				siblingsFieldId,
				numericConstraints);
		for (final SecondaryIndex<?> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			int numMatches = 0;
			if (secondaryIndex.getIndexStrategy() instanceof NumericIndexStrategy) {
				try (final CloseableIterator<ByteArrayId> matches = secondaryIndexQueryManager.query(
						new SpatialQuery(
								queryGeom,
								additionalConstraints),
						secondaryIndex,
						primaryIndex)) {
					while (matches.hasNext()) {
						numMatches++;
						matches.next();
					}
				}
				Assert.assertTrue(
						"Expected 2 but was " + numMatches,
						numMatches == 2);
			}
		}
	}

	@Test
	public void testLessThanOrEqualToConstraint()
			throws IOException {
		final List<FilterableConstraints> numericConstraints = new ArrayList<>();
		final Map<ByteArrayId, List<FilterableConstraints>> additionalConstraints = new HashMap<>();
		numericConstraints.add(new NumericLessThanOrEqualToConstraint(
				siblingsFieldId,
				1));
		additionalConstraints.put(
				siblingsFieldId,
				numericConstraints);
		for (final SecondaryIndex<?> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			int numMatches = 0;
			if (secondaryIndex.getIndexStrategy() instanceof NumericIndexStrategy) {
				try (final CloseableIterator<ByteArrayId> matches = secondaryIndexQueryManager.query(
						new SpatialQuery(
								queryGeom,
								additionalConstraints),
						secondaryIndex,
						primaryIndex)) {
					while (matches.hasNext()) {
						numMatches++;
						matches.next();
					}
				}
				Assert.assertTrue(
						"Expected 2 but was " + numMatches,
						numMatches == 2);
			}
		}
	}

	@Test
	public void testLessThanConstraint()
			throws IOException {
		final List<FilterableConstraints> numericConstraints = new ArrayList<>();
		final Map<ByteArrayId, List<FilterableConstraints>> additionalConstraints = new HashMap<>();
		numericConstraints.add(new NumericLessThanConstraint(
				siblingsFieldId,
				1));
		additionalConstraints.put(
				siblingsFieldId,
				numericConstraints);
		for (final SecondaryIndex<?> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			int numMatches = 0;
			if (secondaryIndex.getIndexStrategy() instanceof NumericIndexStrategy) {
				try (final CloseableIterator<ByteArrayId> matches = secondaryIndexQueryManager.query(
						new SpatialQuery(
								queryGeom,
								additionalConstraints),
						secondaryIndex,
						primaryIndex)) {
					while (matches.hasNext()) {
						numMatches++;
						matches.next();
					}
				}
				Assert.assertTrue(
						"Expected 1 but was " + numMatches,
						numMatches == 1);
			}
		}
	}

	@Test
	public void testEqualsConstraint()
			throws IOException {
		final List<FilterableConstraints> numericConstraints = new ArrayList<>();
		final Map<ByteArrayId, List<FilterableConstraints>> additionalConstraints = new HashMap<>();
		numericConstraints.add(new NumericEqualsConstraint(
				siblingsFieldId,
				1));
		additionalConstraints.put(
				siblingsFieldId,
				numericConstraints);
		for (final SecondaryIndex<?> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			int numMatches = 0;
			if (secondaryIndex.getIndexStrategy() instanceof NumericIndexStrategy) {
				try (final CloseableIterator<ByteArrayId> matches = secondaryIndexQueryManager.query(
						new SpatialQuery(
								queryGeom,
								additionalConstraints),
						secondaryIndex,
						primaryIndex)) {
					while (matches.hasNext()) {
						numMatches++;
						matches.next();
					}
				}
				Assert.assertTrue(
						"Expected 1 but was " + numMatches,
						numMatches == 1);
			}
		}
	}

	@Test
	public void testTemporalQueryConstraint()
			throws IOException,
			ParseException {
		final SimpleDateFormat textFormat = new SimpleDateFormat(
				"yyyy-MM-dd");
		final List<FilterableConstraints> temporalConstraints = new ArrayList<>();
		final Map<ByteArrayId, List<FilterableConstraints>> additionalConstraints = new HashMap<>();
		temporalConstraints.add(new TemporalQueryConstraint(
				birthDateFieldId,
				textFormat.parse("1987-12-12"),
				textFormat.parse("1997-12-12")));
		additionalConstraints.put(
				birthDateFieldId,
				temporalConstraints);
		for (final SecondaryIndex<?> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			int numMatches = 0;
			if (secondaryIndex.getIndexStrategy() instanceof TemporalIndexStrategy) {
				try (final CloseableIterator<ByteArrayId> matches = secondaryIndexQueryManager.query(
						new SpatialQuery(
								queryGeom,
								additionalConstraints),
						secondaryIndex,
						primaryIndex)) {
					while (matches.hasNext()) {
						numMatches++;
						matches.next();
					}
				}
				Assert.assertTrue(
						"Expected 2 but was " + numMatches,
						numMatches == 2);
			}
		}
	}
	
	@Test
	public void testMultipleNumeric() throws IOException {
		final List<FilterableConstraints> numericConstraints = new ArrayList<>();
		final Map<ByteArrayId, List<FilterableConstraints>> additionalConstraints = new HashMap<>();
		numericConstraints.add(new NumericGreaterThanConstraint(
				siblingsFieldId,
				1));
		numericConstraints.add(new NumericLessThanConstraint(
				siblingsFieldId,
				3));
		additionalConstraints.put(
				siblingsFieldId,
				numericConstraints);
		for (final SecondaryIndex<?> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			int numMatches = 0;
			if (secondaryIndex.getIndexStrategy() instanceof NumericIndexStrategy) {
				try (final CloseableIterator<ByteArrayId> matches = secondaryIndexQueryManager.query(
						new SpatialQuery(
								queryGeom,
								additionalConstraints),
						secondaryIndex,
						primaryIndex)) {
					while (matches.hasNext()) {
						numMatches++;
						matches.next();
					}
				}
				Assert.assertTrue(
						"Expected 1 but was " + numMatches,
						numMatches == 1);
			}
		}
	}

	// CONSTANTS
	private static final String NUMERIC_INDEX = "GEOWAVE_2ND_IDX_NUMERIC";
	private static final String TEXT_INDEX = "GEOWAVE_2ND_IDX_NGRAM_2_4";
	private static final String TEMPORAL_INDEX = "GEOWAVE_2ND_IDX_TEMPORAL";

	@BeforeClass
	public static void ingestData()
			throws SchemaException,
			IOException,
			ParseException {

		// define feature type
		final SimpleFeatureType sft = DataUtilities.createType(
				Person.TYPENAME.getValue(),
				Person.SCHEMA.getValue());

		// mark attributes for secondary indexing
		final List<SimpleFeatureUserDataConfiguration> secondaryIndexingConfigs = new ArrayList<>();
		secondaryIndexingConfigs.add(new NumericSecondaryIndexConfiguration(
				Person.SIBLINGS.getValue()));
		secondaryIndexingConfigs.add(new TextSecondaryIndexConfiguration(
				Person.FAVORITES.getValue()));
		secondaryIndexingConfigs.add(new TemporalSecondaryIndexConfiguration(
				Person.BIRTHDATE.getValue()));

		// update schema with 2nd-idx configs
		final SimpleFeatureUserDataConfigurationSet config = new SimpleFeatureUserDataConfigurationSet(
				sft,
				secondaryIndexingConfigs);
		config.updateType(sft);

		final SimpleDateFormat textFormat = new SimpleDateFormat(
				"yyyy-MM-dd");

		// create sample data
		final SimpleFeature ben = buildFeature(
				sft,
				"Ben",
				Location.PITTSBURGH.getPoint(),
				textFormat.parse("1985-12-25"),
				"pizza red baseball",
				0);
		final SimpleFeature bo = buildFeature(
				sft,
				"Bo",
				Location.CHICAGO.getPoint(),
				textFormat.parse("1975-1-1"),
				"spaghetti blue basketball",
				1);
		final SimpleFeature bob = buildFeature(
				sft,
				"Bob",
				Location.MIAMI.getPoint(),
				textFormat.parse("1995-6-30"),
				"hamburgers green football",
				2);
		final SimpleFeature bill = buildFeature(
				sft,
				"Bill",
				Location.PHOENIX.getPoint(),
				textFormat.parse("1990-3-11"),
				"sausage black rugby",
				3);
		features.addAll(Arrays.asList(
				ben,
				bo,
				bob,
				bill));

		// initializations
		adapter = new FeatureDataAdapter(
				sft);
		store = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations),
				accumuloOperations);
		primaryIndex = DEFAULT_SPATIAL_INDEX;
		secondaryIndexQueryManager = new SecondaryIndexQueryManager(
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations));

		// ingest sample data
		try (@SuppressWarnings("unchecked")
		final IndexWriter writer = store.createIndexWriter(
				primaryIndex,
				DataStoreUtils.DEFAULT_VISIBILITY)) {
			for (final SimpleFeature feat : features) {
				System.out.println("Writing feature: " + feat.getAttribute(Person.FIRSTNAME.getValue()));
				writer.write(
						adapter,
						feat);
			}
		}
	}

	private static SimpleFeature buildFeature(
			final SimpleFeatureType sft,
			final String firstName,
			final Point birthPlace,
			final Date birthDate,
			final String favorites,
			final double numSiblings ) {
		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				sft);
		builder.set(
				Person.FIRSTNAME.getValue(),
				firstName);
		builder.set(
				Person.BIRTHPLACE.getValue(),
				birthPlace);
		builder.set(
				Person.BIRTHDATE.getValue(),
				birthDate);
		builder.set(
				Person.FAVORITES.getValue(),
				favorites);
		builder.set(
				Person.SIBLINGS.getValue(),
				numSiblings);
		return builder.buildFeature(UUID.randomUUID().toString());
	}

	private static int countNumberOfEntriesInIndexTable(
			final String tableName )
			throws TableNotFoundException {
		final Scanner scanner = accumuloOperations.createScanner(tableName);
		int numEntries = 0;
		for (@SuppressWarnings("unused")
		final Entry<Key, Value> kv : scanner) {
			numEntries++;
		}
		scanner.close();
		return numEntries;
	}

	private enum Person {

		SCHEMA(
				"firstName:String,birthPlace:Geometry,birthDate:Date,favorites:String,numSiblings:Double"),
		TYPENAME(
				"personInformation"),
		FIRSTNAME(
				"firstName"),
		BIRTHPLACE(
				"birthPlace"),
		BIRTHDATE(
				"birthDate"),
		SIBLINGS(
				"numSiblings"),
		FAVORITES(
				"favorites");

		private final String value;

		private Person(
				final String value ) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}
	}

	private enum Location {

		PITTSBURGH(
				40.431d,
				-80.121d),
		CHICAGO(
				41.834d,
				-88.013d),
		MIAMI(
				25.782d,
				-80.301d),
		PHOENIX(
				33.606d,
				-112.406d),
		HONOLULU(
				21.328d,
				-157.869d),
		MONTREAL(
				45.560d,
				-73.851d);

		private final Point point;

		private Location(
				final double lng,
				final double lat ) {
			point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
					lng,
					lat));
		}

		public Point getPoint() {
			return point;
		}
	}

}
