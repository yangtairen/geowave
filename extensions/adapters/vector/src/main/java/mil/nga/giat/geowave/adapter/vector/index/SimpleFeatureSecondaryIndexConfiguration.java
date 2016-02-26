package mil.nga.giat.geowave.adapter.vector.index;

import org.opengis.feature.simple.SimpleFeature;

import java.util.Set;

import org.codehaus.jackson.annotate.JsonIgnore;

import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration;

public interface SimpleFeatureSecondaryIndexConfiguration extends
		SimpleFeatureUserDataConfiguration
{
	/**
	 * Defines the value of the key to associate with a particular secondary
	 * index configuration within the user data of a {@link SimpleFeature}
	 * 
	 * @return
	 */
	@JsonIgnore
	public String getIndexKey();

	public Set<String> getAttributes();

	public void setAttributes(
			Set<String> attributes );
}
