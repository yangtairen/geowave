package mil.nga.giat.geowave.cli.osm.mapreduce.Ingest;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Mutation;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.ColumnFamily;
import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.ColumnQualifier;
import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.Constants;
import mil.nga.giat.geowave.cli.osm.types.generated.Primitive;
import mil.nga.giat.geowave.cli.osm.types.generated.Relation;
import mil.nga.giat.geowave.cli.osm.types.generated.RelationMember;

/**
 *
 */
public class OSMRelationMapper extends
		OSMMapperBase<Relation>
{
	@Override
	public void map(
			AvroKey<Relation> key,
			NullWritable value,
			Context context )
			throws IOException,
			InterruptedException {

		Relation relation = key.datum();
		Primitive p = relation.getCommon();

		Mutation m = new Mutation(
				getIdHash(p.getId()));

		put(
				m,
				ColumnFamily.RELATION,
				ColumnQualifier.ID,
				p.getId());

		int i = 0;
		for (RelationMember rm : relation.getMembers()) {
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.getRelationMember(
							ColumnQualifier.REFERENCE_ROLEID_PREFIX,
							i),
					rm.getRole());
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.getRelationMember(
							ColumnQualifier.REFERENCE_MEMID_PREFIX,
							i),
					rm.getMember());
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.getRelationMember(
							ColumnQualifier.REFERENCE_TYPE_PREFIX,
							i),
					rm.getMemberType().toString());
			i++;
		}

		if (!Long.valueOf(
				0).equals(
				p.getVersion())) {
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.VERSION,
					p.getVersion());
		}

		if (!Long.valueOf(
				0).equals(
				p.getTimestamp())) {
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.TIMESTAMP,
					p.getTimestamp());
		}

		if (!Long.valueOf(
				0).equals(
				p.getChangesetId())) {
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.CHANGESET,
					p.getChangesetId());
		}

		if (!Long.valueOf(
				0).equals(
				p.getUserId())) {
			put(
					m,
					ColumnFamily.RELATION,
					ColumnQualifier.USER_ID,
					p.getUserId());
		}

		put(
				m,
				ColumnFamily.RELATION,
				ColumnQualifier.USER_TEXT,
				p.getUserName());
		put(
				m,
				ColumnFamily.RELATION,
				ColumnQualifier.OSM_VISIBILITY,
				p.getVisible());

		for (Map.Entry<String, String> kvp : p.getTags().entrySet()) {
			put(
					m,
					ColumnFamily.RELATION,
					kvp.getKey().toString().getBytes(
							Constants.CHARSET),
					kvp.getValue().toString());
		}

		context.write(
				_tableName,
				m);

	}
}
