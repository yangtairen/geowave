package mil.nga.giat.geowave.cli.osm.mapreduce.Ingest;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Mutation;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.Constants;
import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.ColumnQualifier;
import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.ColumnFamily;
import mil.nga.giat.geowave.cli.osm.types.generated.LongArray;
import mil.nga.giat.geowave.cli.osm.types.generated.Primitive;
import mil.nga.giat.geowave.cli.osm.types.generated.Way;

/**
 *
 */
public class OSMWayMapper extends
		OSMMapperBase<Way>
{
	@Override
	public void map(
			AvroKey<Way> key,
			NullWritable value,
			Context context )
			throws IOException,
			InterruptedException {

		Way way = key.datum();
		Primitive p = way.getCommon();

		Mutation m = new Mutation(
				getIdHash(p.getId()));

		put(
				m,
				ColumnFamily.WAY,
				ColumnQualifier.ID,
				p.getId());

		LongArray lr = new LongArray();
		lr.setIds(way.getNodes());

		put(
				m,
				ColumnFamily.WAY,
				ColumnQualifier.REFERENCES,
				lr);

		if (!Long.valueOf(
				0).equals(
				p.getVersion())) {
			put(
					m,
					ColumnFamily.WAY,
					ColumnQualifier.VERSION,
					p.getVersion());
		}

		if (!Long.valueOf(
				0).equals(
				p.getTimestamp())) {
			put(
					m,
					ColumnFamily.WAY,
					ColumnQualifier.TIMESTAMP,
					p.getTimestamp());
		}

		if (!Long.valueOf(
				0).equals(
				p.getChangesetId())) {
			put(
					m,
					ColumnFamily.WAY,
					ColumnQualifier.CHANGESET,
					p.getChangesetId());
		}

		if (!Long.valueOf(
				0).equals(
				p.getUserId())) {
			put(
					m,
					ColumnFamily.WAY,
					ColumnQualifier.USER_ID,
					p.getUserId());
		}

		put(
				m,
				ColumnFamily.WAY,
				ColumnQualifier.USER_TEXT,
				p.getUserName());
		put(
				m,
				ColumnFamily.WAY,
				ColumnQualifier.OSM_VISIBILITY,
				p.getVisible());

		for (Map.Entry<String, String> kvp : p.getTags().entrySet()) {
			put(
					m,
					ColumnFamily.WAY,
					kvp.getKey().toString().getBytes(
							Constants.CHARSET),
					kvp.getValue().toString());
		}

		context.write(
				_tableName,
				m);

	}
}
