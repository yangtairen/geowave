package mil.nga.giat.geowave.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import mil.nga.giat.geowave.test.mapreduce.BasicMapReduceIT;
import mil.nga.giat.geowave.test.mapreduce.BulkIngestInputGenerationIT;
import mil.nga.giat.geowave.test.mapreduce.DBScanIT;
import mil.nga.giat.geowave.test.mapreduce.GeoWaveKMeansIT;
import mil.nga.giat.geowave.test.mapreduce.MapReduceTestEnvironment;
import mil.nga.giat.geowave.test.query.AttributesSubsetQueryIT;
import mil.nga.giat.geowave.test.query.SecondaryIndexingDriver;
import mil.nga.giat.geowave.test.query.SecondaryIndexingQueryTest;
import mil.nga.giat.geowave.test.service.GeoServerIT;
import mil.nga.giat.geowave.test.service.ServicesTestEnvironment;

@RunWith(Suite.class)
@SuiteClasses({
	//GeoWaveBasicIT.class, FIXME currently failing due to assertion failure and resource leakage
	//GeoWaveRasterIT.class, FIXME currently failing due to two NPEs
	// BasicKafkaIT.class, FIXME does not terminate properly due to incorrect kafka ingest & stage command arguments
	BasicMapReduceIT.class,
	BulkIngestInputGenerationIT.class,
	//KDERasterResizeIT.class, FIXME currently failing due to NPE
	GeoWaveKMeansIT.class,
	//GeoWaveNNIT.class, FIXME currently failing due to assertion error
	GeoServerIT.class,
	//GeoWaveServicesIT.class, FIXME does not terminate properly
	//GeoWaveIngestGeoserverIT.class, FIXME currently failing
	AttributesSubsetQueryIT.class,
	SecondaryIndexingDriver.class,
	SecondaryIndexingQueryTest.class,
	DBScanIT.class
})
public class GeoWaveITSuite
{
	@BeforeClass
	public static void setupSuite() {
		synchronized (GeoWaveTestEnvironment.MUTEX) {
			GeoWaveTestEnvironment.DEFER_CLEANUP.set(true);
		}
	}

	@AfterClass
	public static void cleanupSuite() {
		synchronized (GeoWaveTestEnvironment.MUTEX) {
			GeoWaveTestEnvironment.DEFER_CLEANUP.set(false);
			ServicesTestEnvironment.stopServices();
			MapReduceTestEnvironment.cleanupHdfsFiles();
			GeoWaveTestEnvironment.cleanup();
		}
	}
}