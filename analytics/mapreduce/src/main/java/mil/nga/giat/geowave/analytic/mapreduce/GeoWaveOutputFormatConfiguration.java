package mil.nga.giat.geowave.analytic.mapreduce;

import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.StoreParameters;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.output.GeoWaveOutputFormat;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public class GeoWaveOutputFormatConfiguration implements
		FormatConfiguration
{
	/**
	 * Captures the state, but the output format is flexible enough to deal with
	 * both.
	 * 
	 */
	protected boolean isDataWritable = false;

	@Override
	public void setup(
			PropertyManagement runTimeProperties,
			Configuration configuration )
			throws Exception {
		GeoWaveOutputFormat.setAccumuloOperationsInfo(
				configuration,
				runTimeProperties.getPropertyAsString(
						StoreParameters.DataStoreParam.ZOOKEEKER,
						"localhost:2181"),
				runTimeProperties.getPropertyAsString(
						StoreParameters.DataStoreParam.ACCUMULO_INSTANCE,
						"miniInstance"),
				runTimeProperties.getPropertyAsString(
						StoreParameters.DataStoreParam.ACCUMULO_USER,
						"root"),
				runTimeProperties.getPropertyAsString(
						StoreParameters.DataStoreParam.ACCUMULO_PASSWORD,
						"password"),
				runTimeProperties.getPropertyAsString(
						StoreParameters.DataStoreParam.ACCUMULO_NAMESPACE,
						"undefined"));

	}

	@Override
	public Class<?> getFormatClass() {
		return GeoWaveOutputFormat.class;
	}

	@Override
	public boolean isDataWritable() {
		return isDataWritable;
	}

	@Override
	public void setDataIsWritable(
			boolean isWritable ) {
		isDataWritable = isWritable;
	}

	@Override
	public void fillOptions(
			Set<Option> options ) {
		PropertyManagement.fillOptions(
				options,
				new ParameterEnum[] {
					StoreParameters.DataStoreParam.ZOOKEEKER,
					StoreParameters.DataStoreParam.ACCUMULO_INSTANCE,
					StoreParameters.DataStoreParam.ACCUMULO_PASSWORD,
					StoreParameters.DataStoreParam.ACCUMULO_USER,
					StoreParameters.DataStoreParam.ACCUMULO_NAMESPACE
				});
	}
}
