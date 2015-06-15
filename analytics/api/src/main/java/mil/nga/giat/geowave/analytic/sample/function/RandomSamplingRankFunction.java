package mil.nga.giat.geowave.analytic.sample.function;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.mapreduce.JobContext;

/**
 * Pick any object at random by assigning a random weight over a uniform
 * distribution.
 *
 * @param <T>
 */
public class RandomSamplingRankFunction<T> implements
		SamplingRankFunction<T>
{
	private final Random random = new Random();

	@Override
	public void initialize(
			final JobContext context,
			final Class<?> scope )
					throws IOException {}

	@Override
	public double rank(
			final int sampleSize,
			final T value ) {
		return random.nextDouble();
	}
}
