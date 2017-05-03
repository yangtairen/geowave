package mil.nga.giat.geowave.analytic.kryo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import mil.nga.giat.geowave.adapter.vector.FeatureWritable;

public class FeatureSerializer extends
		Serializer<SimpleFeature> {
	final static Logger LOGGER = LoggerFactory.getLogger(FeatureSerializer.class);

	@Override
	public SimpleFeature read(
			final Kryo kryo,
			final Input input,
			final Class<SimpleFeature> feature ) {
		final FeatureWritable fw = new FeatureWritable();
		final byte[] data = input.readBytes(input.readInt());
		try (DataInputStream is = new DataInputStream(
				new ByteArrayInputStream(
						data))) {
			fw.readFields(is);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Cannot deserialize Simple Feature",
					e);
			return null;
		}
		return fw.getFeature();
	}

	@Override
	public void write(
			final Kryo kryo,
			final Output output,
			final SimpleFeature feature ) {

		final FeatureWritable fw = new FeatureWritable(
				feature.getFeatureType());
		fw.setFeature(feature);
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (DataOutputStream os = new DataOutputStream(
				bos)) {
			fw.write(os);
			os.flush();
			final byte[] data = bos.toByteArray();
			output.writeInt(data.length);
			output.write(data);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Cannot serialize Simple Feature",
					e);
		}
	}
}
