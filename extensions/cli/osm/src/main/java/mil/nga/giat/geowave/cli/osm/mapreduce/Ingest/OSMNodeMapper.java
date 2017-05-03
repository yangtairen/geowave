package mil.nga.giat.geowave.cli.osm.mapreduce.Ingest;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Mutation;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.ColumnFamily;
import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.ColumnQualifier;
import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.Constants;
import mil.nga.giat.geowave.cli.osm.types.generated.Node;
import mil.nga.giat.geowave.cli.osm.types.generated.Primitive;

public class OSMNodeMapper extends
		OSMMapperBase<Node>
{
	@Override
	public void map(
			AvroKey<Node> key,
			NullWritable value,
			Context context )
			throws IOException,
			InterruptedException {

		Node node = key.datum();
		Primitive p = node.getCommon();

		Mutation m = new Mutation(
				getIdHash(p.getId()));

		put(
				m,
				ColumnFamily.NODE,
				ColumnQualifier.ID,
				p.getId());
		put(
				m,
				ColumnFamily.NODE,
				ColumnQualifier.LONGITUDE,
				node.getLongitude());
		put(
				m,
				ColumnFamily.NODE,
				ColumnQualifier.LATITUDE,
				node.getLatitude());

		if (!Long.valueOf(
				0).equals(
				p.getVersion())) {
			put(
					m,
					ColumnFamily.NODE,
					ColumnQualifier.VERSION,
					p.getVersion());
		}

		if (!Long.valueOf(
				0).equals(
				p.getTimestamp())) {
			put(
					m,
					ColumnFamily.NODE,
					ColumnQualifier.TIMESTAMP,
					p.getTimestamp());
		}

		if (!Long.valueOf(
				0).equals(
				p.getChangesetId())) {
			put(
					m,
					ColumnFamily.NODE,
					ColumnQualifier.CHANGESET,
					p.getChangesetId());
		}

		if (!Long.valueOf(
				0).equals(
				p.getUserId())) {
			put(
					m,
					ColumnFamily.NODE,
					ColumnQualifier.USER_ID,
					p.getUserId());
		}

		put(
				m,
				ColumnFamily.NODE,
				ColumnQualifier.USER_TEXT,
				p.getUserName());
		put(
				m,
				ColumnFamily.NODE,
				ColumnQualifier.OSM_VISIBILITY,
				p.getVisible());

		for (Map.Entry<String, String> kvp : p.getTags().entrySet()) {
			put(
					m,
					ColumnFamily.NODE,
					kvp.getKey().toString().getBytes(
							Constants.CHARSET),
					kvp.getValue().toString());
		}
		context.write(
				_tableName,
				m);

	}
}
