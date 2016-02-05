package mil.nga.giat.geowave.core.index.lexicoder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public class LongLexicoderTest
{
	private final LongLexicoder longLexicoder = Lexicoders.LONG;

	@Test
	public void testRanges() {
		Assert.assertTrue(
				longLexicoder.getMinimumValue().equals(
						Long.MIN_VALUE));
		Assert.assertTrue(
				longLexicoder.getMaximumValue().equals(
						Long.MAX_VALUE));
	}

	@Test
	public void testSortOrder() {
		final List<Long> values = Arrays.asList(
				-10l,
				Long.MIN_VALUE,
				2678l,
				Long.MAX_VALUE,
				0l);
		final Set<ByteArrayId> lexigraphicallyOrderedBytes = new TreeSet<ByteArrayId>();
		for (final Long l : values) {
			lexigraphicallyOrderedBytes.add(
					new ByteArrayId(
							longLexicoder.toByteArray(
									l)));
		}
		Collections.sort(
				values);
		Assert.assertTrue(
				values.size() == lexigraphicallyOrderedBytes.size());
		int i = 0;
		for (final ByteArrayId bytes : lexigraphicallyOrderedBytes) {
			final Long d = longLexicoder.fromByteArray(
					bytes.getBytes());
			Assert.assertTrue(
					d.equals(
							values.get(
									i++)));
		}
	}
}
