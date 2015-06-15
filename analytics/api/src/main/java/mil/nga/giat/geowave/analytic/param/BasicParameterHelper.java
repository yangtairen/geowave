package mil.nga.giat.geowave.analytic.param;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;

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
			newOption(
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
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final Object value ) {
		setParameter(
				config,
				scope,
				value,
				parent);
	}

	private static final void setParameter(
			final Configuration config,
			final Class<?> clazz,
			final Object val,
			final ParameterEnum configItem ) {
		if (val != null) {
			if (val instanceof Long) {
				config.setLong(
						GeoWaveConfiguratorBase.enumToConfKey(
								clazz,
								configItem.self()),
						((Long) val));
			}
			else if (val instanceof Double) {
				config.setDouble(
						GeoWaveConfiguratorBase.enumToConfKey(
								clazz,
								configItem.self()),
						((Double) val));
			}
			else if (val instanceof Boolean) {
				config.setBoolean(
						GeoWaveConfiguratorBase.enumToConfKey(
								clazz,
								configItem.self()),
						((Boolean) val));
			}
			else if (val instanceof Integer) {
				config.setInt(
						GeoWaveConfiguratorBase.enumToConfKey(
								clazz,
								configItem.self()),
						((Integer) val));
			}
			else if (val instanceof Class) {
				config.setClass(
						GeoWaveConfiguratorBase.enumToConfKey(
								clazz,
								configItem.self()),
						((Class) val),
						((Class) val));
			}
			else if (val instanceof byte[]) {
				config.set(
						GeoWaveConfiguratorBase.enumToConfKey(
								clazz,
								configItem.self()),
						ByteArrayUtils.byteArrayToString(
								(byte[]) val));
			}
			else {
				config.set(
						GeoWaveConfiguratorBase.enumToConfKey(
								clazz,
								configItem.self()),
						val.toString());
			}

		}
	}

	private static final Option newOption(
			final ParameterEnum e,
			final String shortCut,
			final String description,
			final boolean hasArg ) {
		return new Option(
				shortCut,
				toPropertyName(
						e),
				hasArg,
				description);
	}

	private static final String toPropertyName(
			final ParameterEnum param ) {
		return param.getClass().getSimpleName().toLowerCase() + "-" + param.self().name().replace(
				'_',
				'-').toLowerCase();
	}

	@Override
	public Object getValue(
			final JobContext context,
			final Class<?> scope,
			final Object defaultValue ) {
		final ScopedJobConfiguration scopedConfig = new ScopedJobConfiguration(
				context,
				scope);
		if (baseClass.isAssignableFrom(
				Integer.class)) {
			return new Integer(
					scopedConfig.getInt(
							parent.self(),
							((Integer) defaultValue).intValue()));
		}
		else if (baseClass.isAssignableFrom(
				String.class)) {
			return scopedConfig.getString(
					parent.self(),
					defaultValue.toString());
		}else if (baseClass.isAssignableFrom(
				Double.class)) {
			return scopedConfig.getDouble(
					parent.self(),
					(Double)defaultValue);
		}
		else if (baseClass.isAssignableFrom(byte[].class)){
			return scopedConfig.getBytes(
					parent.self());
		}
		else if (baseClass.isAssignableFrom(
				Class.class)) {
			return scopedConfig.getInstance(parent.self(), iface, defaultValue)
					,
					(Double)defaultValue);
		}
		return null;
	}

	@Override
	public Object getValue(
			final CommandLine commandline ) {
		return null;
	}
}
