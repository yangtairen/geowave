package mil.nga.giat.geowave.test.service;

import java.io.File;

import org.junit.Test;

public class ConfProps
{	protected static final String GEOWAVE_WAR_DIR = "target/geowave-services";
	@Test
	public void testConf(){


		final File warDir = new File(
				GEOWAVE_WAR_DIR);

		// update the config file
		File conf = new File(
				warDir,
				"/WEB-INF/config.properties");
	}
}
