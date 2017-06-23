package mil.nga.giat.geowave.datastore.accumulo.metadata;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.metadata.GeoWaveMetadata;
import mil.nga.giat.geowave.datastore.accumulo.BasicOptionProvider;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.MergingCombiner;
import mil.nga.giat.geowave.datastore.accumulo.MergingVisibilityCombiner;

public class AccumuloStatsMetadataWriter extends AccumuloMetadataWriter
{

	private final static Logger LOGGER = Logger.getLogger(AccumuloDataStatisticsStore.class);
	// this is fairly arbitrary at the moment because it is the only custom
	// iterator added
	private static final int STATS_COMBINER_PRIORITY = 10;
	private static final int STATS_MULTI_VISIBILITY_COMBINER_PRIORITY = 15;
	private static final String STATISTICS_COMBINER_NAME = "STATS_COMBINER";
	private static final String STATISTICS_CF = "STATS";
	
	// just attach iterators once per instance
	private boolean iteratorsAttached = false;
	public AccumuloStatsMetadataWriter(
			BatchWriter writer,
			String persistenceTypeName,
			boolean iteratorsAttached) {
		super(
				writer,
				persistenceTypeName);
		this.iteratorsAttached = iteratorsAttached;
	}

	@Override
	public void write(
			GeoWaveMetadata metadata ) {
		super.write(
				metadata);
	}

	protected IteratorConfig[] getIteratorConfig() {
		final Column adapterColumn = new Column(
				STATISTICS_CF);
		final Map<String, String> options = new HashMap<String, String>();
		options.put(
				MergingCombiner.COLUMNS_OPTION,
				ColumnSet.encodeColumns(
						adapterColumn.getFirst(),
						adapterColumn.getSecond()));
		final IteratorConfig statsCombiner = new IteratorConfig(
				EnumSet.allOf(IteratorScope.class),
				STATS_COMBINER_PRIORITY,
				STATISTICS_COMBINER_NAME,
				MergingCombiner.class.getName(),
				new BasicOptionProvider(
						options));
		return new IteratorConfig[] {
			statsCombiner
		};
	}

	protected IteratorSetting[] getScanSettings() {
		final IteratorSetting statsMultiVisibilityCombiner = new IteratorSetting(
				STATS_MULTI_VISIBILITY_COMBINER_PRIORITY,
				MergingVisibilityCombiner.class);
		return new IteratorSetting[] {
			statsMultiVisibilityCombiner
		};
	}

}
