package mil.nga.giat.geowave.format.tracks2;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import mil.nga.giat.geowave.format.geotools.vector.RetypingVectorDataPlugin.RetypingVectorDataSource;

public class TracksRetypingSource implements
		RetypingVectorDataSource
{
	private SimpleFeatureType typeIn;

	public TracksRetypingSource(
			SimpleFeatureType typeIn ) {
		this.typeIn = typeIn;
	}

	@Override
	public SimpleFeatureType getRetypedSimpleFeatureType() {
		SimpleFeatureTypeBuilder typeOutBuilder = new SimpleFeatureTypeBuilder();
		typeOutBuilder.init(typeIn);
		typeOutBuilder.setName("track_benchmark");
		int index = typeIn.indexOf("datetime");
		AttributeDescriptor timeDesc = typeOutBuilder.remove("datetime");
		AttributeTypeBuilder newTimeBuilder = new AttributeTypeBuilder();
		newTimeBuilder.init(timeDesc);
		newTimeBuilder.setBinding(Date.class);
		typeOutBuilder.add(
				index,
				newTimeBuilder.buildDescriptor(timeDesc.getLocalName()));

		final SimpleFeatureType typeOut = typeOutBuilder.buildFeatureType();
		return typeOut;
	}

	@Override
	public SimpleFeature getRetypedSimpleFeature(
			SimpleFeatureBuilder retypeBuilder,
			SimpleFeature original ) {
		retypeBuilder.init(original);
		retypeBuilder.set(
				"datetime",
				new Date(
						(Long) original.getAttribute("datetime")));
		return retypeBuilder.buildFeature(original.getID());
	}

}
