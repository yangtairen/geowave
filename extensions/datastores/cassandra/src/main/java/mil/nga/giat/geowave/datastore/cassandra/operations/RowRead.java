package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.util.Iterator;

import org.apache.commons.collections.iterators.EmptyIterator;

import com.datastax.driver.core.PreparedStatement;

import mil.nga.giat.geowave.datastore.cassandra.CassandraIndexWriter;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;

public class RowRead extends
		BatchHandler
{
	private final CassandraOperations operations;
	private final PreparedStatement preparedRead;

	public RowRead(
			final PreparedStatement preparedRead,
			final CassandraOperations operations ) {
		super(
				operations.getSession());
		this.preparedRead = preparedRead;
		this.operations = operations;
	}

	public void setRow(
			final byte[] row ) {
		for (int p = 0; p < CassandraIndexWriter.PARTITIONS; p++) {

		}
	}

	public CassandraRow result() {
		return null;
	}
}
