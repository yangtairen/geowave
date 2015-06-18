package mil.nga.giat.geowave.analytic;

import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public interface IndependentJobRunner
{
	public void fillOptions(
			Set<Option> options );

	public int run(
			CommandLine commandLine )
			throws Exception;
}
