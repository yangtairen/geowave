package mil.nga.giat.geowave.core.index.lexicoder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.junit.Assert;
import org.junit.Test;

public class DoubleLexicoderTest
{
	private final DoubleLexicoder doubleLexicoder = Lexicoders.DOUBLE;

	@Test
	public void testRanges() {
		Assert.assertTrue(doubleLexicoder.getMinimumValue().equals(
				(double) Long.MIN_VALUE));
		Assert.assertTrue(doubleLexicoder.getMaximumValue().equals(
				Double.MAX_VALUE));
	}

	@Test
	public void testSortOrder() {
		final List<Double> doubleList = Arrays.asList(
				-10d,
				Double.MIN_VALUE,
				11d,
				-14.2,
				14.2,
				-100.002,
				100.002,
				-11d,
				Double.MAX_VALUE,
				0d);
		final Set<ByteArrayId> lexigraphicallyOrderedBytes = new TreeSet<ByteArrayId>();
		for (Double d : doubleList) {
			lexigraphicallyOrderedBytes.add(new ByteArrayId(
					doubleLexicoder.toByteArray(d)));
		}
		Collections.sort(doubleList);
		Assert.assertTrue(doubleList.size() == lexigraphicallyOrderedBytes.size());
		int i = 0;
		for (ByteArrayId bytes : lexigraphicallyOrderedBytes) {
			Double d = doubleLexicoder.fromByteArray(bytes.getBytes());
			Assert.assertTrue(d.equals(doubleList.get(i++)));
		}
	}
}
