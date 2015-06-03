package mil.nga.giat.geowave.core.cli;

import org.apache.commons.cli.ParseException;

public interface CLIOperationDriver
{
	public void runOperation(
			final String[] args )
			throws ParseException;
}
