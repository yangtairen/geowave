package mil.nga.giat.geowave.core.geotime.store.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryAdapter;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class SpatialQueryTest
{
	@Test
	public void test() {
		final GeometryFactory factory = new GeometryFactory();
		final SpatialQuery query = new SpatialQuery(
				factory.createPolygon(new Coordinate[] {
					new Coordinate(
							24,
							33),
					new Coordinate(
							28,
							33),
					new Coordinate(
							28,
							31),
					new Coordinate(
							24,
							31),
					new Coordinate(
							24,
							33)
				}));
		final SpatialQuery queryCopy = new SpatialQuery();
		queryCopy.fromBinary(query.toBinary());
		assertEquals(
				queryCopy.getQueryGeometry(),
				query.getQueryGeometry());
	}

	private IndexedPersistenceEncoding createData(
			Coordinate[] coordinates ) {
		GeometryFactory factory = new GeometryFactory();
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();

		commonData.addOrUpdateValue(new PersistentValue<CommonIndexValue>(
				GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID,
				new GeometryWrapper(
						factory.createLineString(coordinates))));

		return new IndexedPersistenceEncoding(
				new ByteArrayId(
						"1"),
				new ByteArrayId(
						"1"),
				new ByteArrayId(
						"1"),
				1,
				commonData);
	}

	public void performOp(
			CompareOperation op,
			boolean[] expectedResults ) {
		final GeometryFactory factory = new GeometryFactory();
		final SpatialQuery query = new SpatialQuery(
				factory.createPolygon(new Coordinate[] {
					new Coordinate(
							24,
							33),
					new Coordinate(
							28,
							33),
					new Coordinate(
							28,
							37),
					new Coordinate(
							24,
							37),
					new Coordinate(
							24,
							33)
				}),
				op);
		final SpatialQuery queryCopy = new SpatialQuery();
		queryCopy.fromBinary(query.toBinary());

		IndexedPersistenceEncoding[] data = new IndexedPersistenceEncoding[] {
			createData(new Coordinate[] {
				new Coordinate(
						22,
						32),
				new Coordinate(
						25,
						36)
			}),
			createData(new Coordinate[] {
				new Coordinate(
						25,
						33.5),
				new Coordinate(
						26,
						34)
			}),
			createData(new Coordinate[] {
				new Coordinate(
						21,
						33.5),
				new Coordinate(
						23,
						34)
			}),
			createData(new Coordinate[] {
				new Coordinate(
						29,
						33.5),
				new Coordinate(
						30,
						34)
			})
		};

		int pos = 0;
		for (IndexedPersistenceEncoding dataItem : data)
			for (QueryFilter filter : queryCopy.createFilters(IndexType.SPATIAL_VECTOR.getDefaultIndexModel())) {
				assertEquals(
						"result: " + pos,
						expectedResults[pos++],
						filter.accept(dataItem));
			}
	}

	@Test
	public void testContains() {
		performOp(
				CompareOperation.CONTAINS,
				new boolean[] {
					false,
					true,
					false,
					false
				});
	}

	@Test
	public void testOverlaps() {
		performOp(
				CompareOperation.OVERLAPS,
				new boolean[] {
					true,
					true,
					false,
					false
				});
	}
}
