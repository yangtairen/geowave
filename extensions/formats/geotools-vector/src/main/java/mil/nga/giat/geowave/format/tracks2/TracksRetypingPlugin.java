package mil.nga.giat.geowave.format.tracks2;

import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.format.geotools.vector.RetypingVectorDataPlugin;

public class TracksRetypingPlugin implements
		RetypingVectorDataPlugin
{

	@Override
	public RetypingVectorDataSource getRetypingSource(
			SimpleFeatureType type ) {
		return new TracksRetypingSource();
	}

}
