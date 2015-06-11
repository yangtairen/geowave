package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public class BasicParameterHelper implements
		ParameterHelper<Object>
{
	private final ParameterEnum parent;
	private final Class<Object> baseClass;
	private final Option[] options;

	public BasicParameterHelper(
			final ParameterEnum parent,
			final Class<Object> baseClass,
			final String name,
			final String description,
			final boolean hasArg ) {
		this.baseClass = baseClass;
		this.parent = parent;
		options = new Option[] {
			PropertyManagement.newOption(
					parent,
					name,
					description,
					hasArg)
		};
	}

	@Override
	public Class<Object> getBaseClass() {
		return baseClass;
	}

	@Override
	public Option[] getOptions() {
		return options;
	}

	@Override
	public void setParameter(
			final Configuration jobConfig,
			final Class<?> jobScope,
			final PropertyManagement propertyValues ) {
		RunnerUtils.setParameter(
				jobConfig,
				jobScope,
				propertyValues,
				parent);
	}

	@Override
	public Object getValue(
			final ConfigurationWrapper config,
			final Object defaultValue ) {
		if (baseClass.isAssignableFrom(Integer.class)) {
			return new Integer(
					config.getInt(
							parent.self(),
							((Integer) defaultValue).intValue()));
		}
		return null;
	}
}
