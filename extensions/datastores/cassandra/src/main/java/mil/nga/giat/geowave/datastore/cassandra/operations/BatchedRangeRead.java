package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.util.Iterator;

import org.apache.commons.collections.iterators.EmptyIterator;

import com.datastax.driver.core.PreparedStatement;

import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.datastore.cassandra.CassandraIndexWriter;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;

public class BatchedRangeRead extends
		BatchHandler
{
	private final CassandraOperations operations;
	private final PreparedStatement preparedRead;

	public BatchedRangeRead(
			final PreparedStatement preparedRead,
			final CassandraOperations operations ) {
		super(
				operations.getSession());
		this.preparedRead = preparedRead;
		this.operations = operations;
	}

	public void addQueryRange(
			final ByteArrayRange range ) {
		for (int p = 0; p < CassandraIndexWriter.PARTITIONS; p++) {

		}
	}

	public Iterator<CassandraRow> results() {
		return EmptyIterator.INSTANCE;
	}
}
