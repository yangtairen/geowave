package mil.nga.giat.geowave.analytic;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;

import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScopedJobConfiguration
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(ScopedJobConfiguration.class);

	private final JobContext context;

	private final Class<?> scope;
	private Logger logger = LOGGER;

	public ScopedJobConfiguration(
			final JobContext context,
			final Class<?> scope ) {
		super();
		this.context = context;
		this.scope = scope;
	}

	public ScopedJobConfiguration(
			final JobContext context,
			final Class<?> scope,
			final Logger logger ) {
		super();
		this.context = context;
		this.scope = scope;
		this.logger = logger;
	}

	public int getInt(
			final Enum<?> property,
			final int defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (context.getConfiguration().getRaw(
				propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		final int v = context.getConfiguration().getInt(
				propName,
				defaultValue);
		return v;
	}

	public String getString(
			final Enum<?> property,
			final String defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (context.getConfiguration().getRaw(
				propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		return context.getConfiguration().get(
				propName,
				defaultValue);
	}

	public <T> T getInstance(
			final Enum<?> property,
			final Class<T> iface,
			final Class<? extends T> defaultValue )
			throws InstantiationException,
			IllegalAccessException {
		try {
			final String propName = GeoWaveConfiguratorBase.enumToConfKey(
					scope,
					property);
			if (context.getConfiguration().getRaw(
					propName) == null) {
				if (defaultValue == null) {
					return null;
				}
				logger.warn("Using default for property " + propName);
			}
			return GeoWaveConfiguratorBase.getInstance(
					scope,
					property,
					context,
					iface,
					defaultValue);
		}
		catch (final Exception ex) {
			logger.error("Cannot instantiate " + GeoWaveConfiguratorBase.enumToConfKey(
					scope,
					property));
			throw ex;
		}
	}

	public double getDouble(
			final Enum<?> property,
			final double defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (context.getConfiguration().getRaw(
				propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		return context.getConfiguration().getDouble(
				propName,
				defaultValue);
	}

	public byte[] getBytes(
			final Enum<?> property ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		final String data = context.getConfiguration().getRaw(
				propName);
		if (data == null) {
			logger.error(propName + " not found ");
		}
		return ByteArrayUtils.byteArrayFromString(data);
	}

}