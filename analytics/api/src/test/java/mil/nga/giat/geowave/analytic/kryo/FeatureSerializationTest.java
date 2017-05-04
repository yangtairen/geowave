package mil.nga.giat.geowave.analytic.kryo;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.UUID;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class FeatureSerializationTest
{

	@Test
	public void test()
			throws SchemaException {
		final Kryo kryo = new Kryo();

		kryo.register(
				SimpleFeatureImpl.class,
				new FeatureSerializer());

		final SimpleFeatureType schema = DataUtilities.createType(
				"testGeo",
				"location:Point:srid=4326,name:String");
		final List<AttributeDescriptor> descriptors = schema.getAttributeDescriptors();
		final Object[] defaults = new Object[descriptors.size()];
		int p = 0;
		for (final AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

		final SimpleFeature feature = SimpleFeatureBuilder.build(
				schema,
				defaults,
				UUID.randomUUID().toString());
		final GeometryFactory geoFactory = new GeometryFactory();

		feature.setAttribute(
				"location",
				geoFactory.createPoint(new Coordinate(
						-45,
						45)));

		// by registering the SimpleFeatureImpl class with the FeatureSerializer
		// serializer class, kryo will automatically
		// use that serializer, so no need to specify the extra parameters in
		// the read & write commands

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		Output output = new Output(
				bos);
		kryo.writeObject(
				output,
				feature);
		output.flush();
		output.close();

		final SimpleFeature f2 = kryo.readObject(
				new Input(
						new ByteArrayInputStream(
								bos.toByteArray())),
				SimpleFeatureImpl.class);
		assertEquals(
				feature,
				f2);
	}
}
