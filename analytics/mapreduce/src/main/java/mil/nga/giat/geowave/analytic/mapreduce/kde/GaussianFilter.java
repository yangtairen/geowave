package mil.nga.giat.geowave.analytic.mapreduce.kde;

import java.util.ArrayList;
import java.util.List;

public class GaussianFilter
{
	private static final double SQRT_2_PI = Math.sqrt(2 * Math.PI);

	/**
	 * This kernel was computed with sigma = 1 for x=(-3,-2,-1,0,1,2,3)
	 */
	private static double[] majorSmoothingGaussianKernel = new double[] {
		0.006,
		0.061,
		0.242,
		0.383,
		0.242,
		0.061,
		0.006
	};

	private static class ValueRange
	{
		private final double min;
		private final double max;

		private ValueRange(
				final double min,
				final double max ) {
			this.min = min;
			this.max = max;
		}

		public double getMin() {
			return min;
		}

		public double getMax() {
			return max;
		}
	}

	private static final ValueRange[] valueRangePerDimension = new ValueRange[] {
		new ValueRange(
				-180,
				180),
		new ValueRange(
				-90,
				90)
	};

	public static void incrementPt(
			final double lat,
			final double lon,
			final CellCounter results,
			final int numXPosts,
			final int numYPosts ) {
		incrementBBox(
				lon,
				lon,
				lat,
				lat,
				results,
				numXPosts,
				numYPosts,
				1);
	}

	public static void incrementPt(
			final double lat,
			final double lon,
			final CellCounter results,
			final int numXPosts,
			final int numYPosts,
			double contributionScaleFactor ) {
		incrementBBox(
				lon,
				lon,
				lat,
				lat,
				results,
				numXPosts,
				numYPosts,
				contributionScaleFactor);
	}

	public static void incrementPtFast(
			final double lat,
			final double lon,
			final CellCounter results,
			final int numXPosts,
			final int numYPosts ) {
		final int numDimensions = 2;
		final double[] binLocationPerDimension = new double[numDimensions];
		final int[] binsPerDimension = new int[] {
			numXPosts,
			numYPosts
		};
		final double[] valsPerDimension = new double[] {
			lon,
			lat
		};
		for (int d = 0; d < numDimensions; d++) {
			final ValueRange valueRange = valueRangePerDimension[d];
			final double span = (valueRange.getMax() - valueRange.getMin());
			binLocationPerDimension[d] = (((valsPerDimension[d] - valueRange.getMin()) / span) * binsPerDimension[d]);
		}
		final double[] gaussianKernel = getGaussianKernel(
				1,
				3);
		final int maxOffset = gaussianKernel.length / 2;
		final List<int[]> offsets = getOffsets(
				numDimensions,
				0,
				new int[numDimensions],
				gaussianKernel,
				maxOffset);
		for (final int[] offset : offsets) {
			final double blur = getBlurFromOffset(
					offset,
					gaussianKernel,
					maxOffset);
			final List<BinPositionAndContribution> positionsAndContributions = getPositionsAndContributionPt(
					numDimensions,
					0,
					binLocationPerDimension,
					blur,
					new int[numDimensions],
					binsPerDimension,
					offset);
			for (final BinPositionAndContribution positionAndContribution : positionsAndContributions) {
				results.increment(
						positionAndContribution.position,
						positionAndContribution.contribution);
			}
		}
	}

	public static void incrementBBox(
			final double minX,
			final double maxX,
			final double minY,
			final double maxY,
			final CellCounter results,
			final int numXPosts,
			final int numYPosts,
			double contributionScaleFactor ) {
		final int numDimensions = 2;
		final double[] minBinLocationPerDimension = new double[numDimensions];
		final double[] maxBinLocationPerDimension = new double[numDimensions];
		final int[] binsPerDimension = new int[] {
			numXPosts,
			numYPosts
		};
		final ValueRange[] valueRangePerDimension = new ValueRange[] {
			new ValueRange(
					-180,
					180),
			new ValueRange(
					-90,
					90)
		};
		final double[] minsPerDimension = new double[] {
			minX,
			minY
		};
		final double[] maxesPerDimension = new double[] {
			maxX,
			maxY
		};
		for (int d = 0; d < numDimensions; d++) {
			final ValueRange valueRange = valueRangePerDimension[d];
			final double span = (valueRange.getMax() - valueRange.getMin());
			minBinLocationPerDimension[d] = (((minsPerDimension[d] - valueRange.getMin()) / span) * binsPerDimension[d]);
			maxBinLocationPerDimension[d] = (((maxesPerDimension[d] - valueRange.getMin()) / span) * binsPerDimension[d]);
			// give it a buffer of 1 for being counted within this bounds
			// because we perform smoothing on the values anyway
			if ((maxBinLocationPerDimension[d] < -1) || (minBinLocationPerDimension[d] > binsPerDimension[d])) {
				// not in bounds
				return;
			}
			else {
				minBinLocationPerDimension[d] = Math.max(
						minBinLocationPerDimension[d],
						-1);
				maxBinLocationPerDimension[d] = Math.min(
						maxBinLocationPerDimension[d],
						binsPerDimension[d]);
			}

		}
		final double[] gaussianKernel = getGaussianKernel(
				1,
				3);
		final int maxOffset = gaussianKernel.length / 2;
		final List<int[]> offsets = getOffsets(
				numDimensions,
				0,
				new int[numDimensions],
				gaussianKernel,
				maxOffset);
		for (final int[] offset : offsets) {
			final double blur = getBlurFromOffset(
					offset,
					gaussianKernel,
					maxOffset);
			final List<BinPositionAndContribution> positionsAndContributions = getPositionsAndContribution(
					numDimensions,
					0,
					minBinLocationPerDimension,
					maxBinLocationPerDimension,
					blur,
					new int[numDimensions],
					binsPerDimension,
					offset);
			for (final BinPositionAndContribution positionAndContribution : positionsAndContributions) {
				results.increment(
						positionAndContribution.position,
						positionAndContribution.contribution * contributionScaleFactor);
			}
		}
	}

	protected static double getSigma(
			final int radius,
			final int order ) {
		return ((radius * 2.0) + 1.0) / (5.0 + (0.8 * order));
	}

	protected static double[] getGaussianKernel(
			final double sigma,
			final int radius ) {
		return majorSmoothingGaussianKernel;
	}

	protected static double computePDF(
			final double mean,
			final double sigma,
			final double sample ) {
		final double delta = sample - mean;
		return Math.exp((-delta * delta) / (2.0 * sigma * sigma)) / (sigma * SQRT_2_PI);
	}

	protected static double[] normalizeSumToOne(
			final double[] kernel ) {
		final double[] retVal = new double[kernel.length];
		double total = 0;
		for (final double element : kernel) {
			total += element;
		}
		for (int i = 0; i < kernel.length; i++) {
			retVal[i] = kernel[i] / total;
		}
		return retVal;
	}

	static private List<int[]> getOffsets(
			final int numDimensions,
			final int currentDimension,
			final int[] currentOffsetsPerDimension,
			final double[] gaussianKernel,
			final int maxOffset ) {
		final List<int[]> offsets = new ArrayList<int[]>();
		if (currentDimension == numDimensions) {
			offsets.add(currentOffsetsPerDimension.clone());
		}
		else {
			for (int i = -maxOffset; i < (gaussianKernel.length - maxOffset); i++) {
				currentOffsetsPerDimension[currentDimension] = i;
				offsets.addAll(getOffsets(
						numDimensions,
						currentDimension + 1,
						currentOffsetsPerDimension,
						gaussianKernel,
						maxOffset));
			}
		}
		return offsets;
	}

	static private double getBlurFromOffset(
			final int[] indexIntoGaussianPerDimension,
			final double[] gaussianKernel,
			final int maxOffset ) {
		double blurFactor = 1;

		for (final int index : indexIntoGaussianPerDimension) {
			blurFactor *= gaussianKernel[index + maxOffset];
		}
		return blurFactor;
	}

	private static List<BinPositionAndContribution> getPositionsAndContributionPt(
			final int numDimensions,
			final int currentDimension,
			final double[] locationPerDimension,
			final double currentContribution,
			final int[] finalIndexPerDimension,
			final int[] binsPerDimension,
			final int[] offset ) {
		final List<BinPositionAndContribution> positions = new ArrayList<BinPositionAndContribution>();
		if (currentDimension == numDimensions) {
			positions.add(new BinPositionAndContribution(
					getPosition(
							finalIndexPerDimension,
							binsPerDimension),
					currentContribution));
		}
		else {
			final int floorOfLocation = (int) (locationPerDimension[currentDimension]);
			final int[] floorLocation = finalIndexPerDimension;
			floorLocation[currentDimension] = floorOfLocation + offset[currentDimension];
			if ((floorLocation[currentDimension] >= 0)
					&& (floorLocation[currentDimension] < binsPerDimension[currentDimension])) {
				positions.addAll(getPositionsAndContributionPt(
						numDimensions,
						currentDimension + 1,
						locationPerDimension,
						currentContribution,
						floorLocation,
						binsPerDimension,
						offset));
			}
		}
		return positions;
	}

	private static List<BinPositionAndContribution> getPositionsAndContribution(
			final int numDimensions,
			final int currentDimension,
			final double[] minLocationPerDimension,
			final double[] maxLocationPerDimension,
			final double currentContribution,
			final int[] finalIndexPerDimension,
			final int[] binsPerDimension,
			final int[] offset ) {
		final List<BinPositionAndContribution> positions = new ArrayList<BinPositionAndContribution>();
		if (currentDimension == numDimensions) {
			positions.add(new BinPositionAndContribution(
					getPosition(
							finalIndexPerDimension,
							binsPerDimension),
					currentContribution));
		}
		else {
			final int floorOfLocation = (int) (minLocationPerDimension[currentDimension]);
			final int[] floorLocation = finalIndexPerDimension.clone();
			floorLocation[currentDimension] = floorOfLocation + offset[currentDimension];
			if ((floorLocation[currentDimension] >= 0)
					&& (floorLocation[currentDimension] < binsPerDimension[currentDimension])) {
				positions.addAll(getPositionsAndContribution(
						numDimensions,
						currentDimension + 1,
						minLocationPerDimension,
						maxLocationPerDimension,
						currentContribution,
						floorLocation,
						binsPerDimension,
						offset));
			}
			final int ceilOfLocation = (int) Math.ceil(maxLocationPerDimension[currentDimension]);
			/**
			 * the exterior cells are covered above by the floor of the min and
			 * ceil of the max, everything in between is covered below
			 */
			final int startLocation = Math.max(
					floorOfLocation + offset[currentDimension] + 1,
					0);
			final int stopLocation = Math.min(
					ceilOfLocation + offset[currentDimension],
					binsPerDimension[currentDimension]);
			if (startLocation < stopLocation) {
				for (int location = startLocation; location < stopLocation; location++) {
					final int[] middleLocation = finalIndexPerDimension.clone();
					middleLocation[currentDimension] = location;
					positions.addAll(getPositionsAndContribution(
							numDimensions,
							currentDimension + 1,
							minLocationPerDimension,
							maxLocationPerDimension,
							currentContribution,
							middleLocation,
							binsPerDimension,
							offset));
				}
			}
		}
		return positions;
	}

	private static long getPosition(
			final int[] positionPerDimension,
			final int[] binsPerDimension ) {
		long retVal = 0;
		double multiplier = 1;
		for (int d = positionPerDimension.length - 1; d >= 0; d--) {
			retVal += (positionPerDimension[d] * multiplier);
			multiplier *= binsPerDimension[d];
		}
		return retVal;
	}

	private static class BinPositionAndContribution
	{
		final private long position;
		final private double contribution;

		private BinPositionAndContribution(
				final long position,
				final double contribution ) {
			this.position = position;
			this.contribution = contribution;
		}
	}
}
