package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public interface ParameterEnum
{
	public Class<?> getBaseClass();
	
	public Option[] getOptions();

	public void setParameter(
			final Configuration jobConfig,
			final Class<?> jobScope,
			final PropertyManagement propertyValues );

	public Enum<?> self();
}
