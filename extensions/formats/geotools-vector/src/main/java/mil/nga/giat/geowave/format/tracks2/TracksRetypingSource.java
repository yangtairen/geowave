package mil.nga.giat.geowave.format.tracks2;

import java.util.Date;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import mil.nga.giat.geowave.format.geotools.vector.RetypingVectorDataPlugin.RetypingVectorDataSource;

public class TracksRetypingSource implements
		RetypingVectorDataSource
{

	Tracks2IngestFormat typeIn = new Tracks2IngestFormat();

	@Override
	public SimpleFeatureType getRetypedSimpleFeatureType() {

		SimpleFeatureTypeBuilder typeOutBuilder = new SimpleFeatureTypeBuilder();
		typeOutBuilder.description(typeIn.getIngestFormatDescription());
		typeOutBuilder.setName(typeIn.getIngestFormatName());

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
