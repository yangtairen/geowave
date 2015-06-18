package mil.nga.giat.geowave.analytic.param;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

public interface ParameterHelper<T>
{
	public Class<T> getBaseClass();

	public Option[] getOptions();

	public T getValue(
			CommandLine commandline )
			throws ParseException;

	public void setValue(
			Configuration config,
			Class<?> scope,
			T value );

	public T getValue(
			JobContext context,
			Class<?> scope,
			T defaultValue );
}
