package mil.nga.giat.geowave.format.tracks2;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.format.geotools.vector.GeoToolsVectorDataOptions;
import mil.nga.giat.geowave.format.geotools.vector.GeoToolsVectorDataStoreIngestFormat;
import mil.nga.giat.geowave.format.geotools.vector.GeoToolsVectorDataStoreIngestPlugin;
import mil.nga.giat.geowave.format.geotools.vector.retyping.date.DateFieldRetypingPlugin;

public class Tracks2IngestFormat implements
		IngestFormatPluginProviderSpi<Object, SimpleFeature>
{
	@Override
	public AvroFormatPlugin<Object, SimpleFeature> createAvroFormatPlugin(
			IngestFormatOptionProvider options ) {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools vector files cannot be ingested using intermediate avro files");
	}

	@Override
	public IngestFromHdfsPlugin<Object, SimpleFeature> createIngestFromHdfsPlugin(
			IngestFormatOptionProvider options ) {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools vector files cannot be ingested from HDFS");
	}

	@Override
	public LocalFileIngestPlugin<SimpleFeature> createLocalFileIngestPlugin(
			IngestFormatOptionProvider options ) {
		GeoToolsVectorDataOptions vectorDataOptions = (GeoToolsVectorDataOptions) options;
		return new GeoToolsVectorDataStoreIngestPlugin(
				new TracksRetypingPlugin(),
				vectorDataOptions.getCqlFilterOptionProvider(),
				vectorDataOptions.getFeatureTypeNames());
	}

	@Override
	public String getIngestFormatName() {
		return "tracks";
	}

	@Override
	public String getIngestFormatDescription() {
		return "test";
	}

	@Override
	public IngestFormatOptionProvider createOptionsInstances() {
		return new GeoToolsVectorDataOptions();
	}
}
