package mil.nga.giat.geowave.analytic;

import java.util.Collection;

import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyticCLIOperationDriver implements
		CLIOperationDriver
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticCLIOperationDriver.class);
	private final IndependentJobRunner jobRunner;

	public AnalyticCLIOperationDriver(
			final IndependentJobRunner jobRunner ) {
		super();
		this.jobRunner = jobRunner;
	}

	@Override
	public void runOperation(
			final String[] args )
			throws ParseException {
		final Options options = new Options();
		final OptionGroup baseOptionGroup = new OptionGroup();
		baseOptionGroup.setRequired(false);
		baseOptionGroup.addOption(new Option(
				"h",
				"help",
				false,
				"Display help"));
		options.addOptionGroup(baseOptionGroup);

		final Collection<ParameterEnum<?>> params = jobRunner.getParameters();

		for (final ParameterEnum<?> param : params) {
			final Option[] paramOptions = param.getHelper().getOptions();
			for (final Option o : paramOptions) {
				options.addOption(o);
			}
		}

		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				options,
				args);
		if (commandLine.hasOption("h")) {
			printHelp(options);
			return;
		}
		else {
			try {
				final PropertyManagement properties = new PropertyManagement();
				for (final ParameterEnum<?> param : params) {
					final Object value = param.getHelper().getValue(
							commandLine);
					((ParameterEnum<Object>) param).getHelper().setValue(
							properties,
							value);
				}
				jobRunner.run(properties);
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to run analytic job",
						e);
				return;
			}
		}
	}

	private static void printHelp(
			final Options options ) {
		final HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(
				"Analytics",
				"\nOptions:",
				options,
				"");
	}
}
