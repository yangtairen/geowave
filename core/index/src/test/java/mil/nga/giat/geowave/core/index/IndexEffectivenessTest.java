package mil.nga.giat.geowave.core.index;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;

public class IndexEffectivenessTest
{

	private static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new BasicDimensionDefinition(
				-90,
				90),
		new BasicDimensionDefinition(
				-90,
				90)
	};

	private static final NumericIndexStrategy sfcIndexStrategy = TieredSFCIndexFactory.createSingleTierStrategy(
			SPATIAL_DIMENSIONS,
			new int[] {
				16,
				16
	},
			SFCType.HILBERT);

	public interface DoOp
	{
		public void run(
				NumericData[] set );
	}

	protected interface BasicQueryCompareOp
	{
		public boolean compare(
				double dataMin,
				double dataMax,
				double queryMin,
				double queryMax );
	}

	public enum BasicQueryCompareOperation implements
		BasicQueryCompareOp {
			CONTAINS {
				@Override
				public boolean compare(
						double dataMin,
						double dataMax,
						double queryMin,
						double queryMax ) {
					return !((dataMin < queryMin) || (dataMax > queryMax));
				}
			},
			OVERLAPS {
				@Override
				public boolean compare(
						double dataMin,
						double dataMax,
						double queryMin,
						double queryMax ) {
					return !((dataMax < queryMin) || (dataMin > queryMax));
				}
			}
	};

	public static class Region
	{
		static final Map<BasicNumericDataset, List<ByteArrayId>> idsPerEntry = new HashMap<BasicNumericDataset, List<ByteArrayId>>();
		NumericData[] query;

		@Override
		public String toString() {
			return "Region [query=" + Arrays.toString(
					query) + "]";
		}

		public Region(
				NumericData[] query ) {
			super();
			this.query = Arrays.copyOf(
					query,
					query.length);
		}

		public void add(
				BasicNumericDataset dataSet,
				List<ByteArrayId> ids ) {
			List<ByteArrayId> regionIds = idsPerEntry.get(
					dataSet);
			if (regionIds == null) {
				regionIds = new ArrayList<ByteArrayId>();
				idsPerEntry.put(
						dataSet,
						regionIds);
			}
			regionIds.addAll(
					ids);
		}

		public ResultPair getResults(
				NumericIndexStrategy strategy,
				List<ByteArrayRange> queryRanges ) {
			int tp = 0;
			int fp = 0;
			int fn = 0;
			int tn = 0;
			for (Map.Entry<BasicNumericDataset, List<ByteArrayId>> entry : idsPerEntry.entrySet()) {
				final boolean intersects = this.contains(
						entry.getKey());
				for (ByteArrayId id : entry.getValue()) {
					boolean found = false;
					for (ByteArrayRange range : queryRanges) {
						found |= (range.getStart().compareTo(
								id) <= 0
								&& range.getEnd().compareTo(
										id) >= 0);
					}
					if (found) {
						if (intersects)
							tp++;
						else
							fp++;
					}
					else {
						if (intersects)
							fn++;
						else
							tn++;
					}
				}
			}
			return new ResultPair(
					this.query,
					tp,
					tn,
					fp,
					fn);
		}

		public boolean contains(
				BasicNumericDataset dataSet ) {
			for (int i = 0; i < dataSet.getDimensionCount(); i++) {
				if (!BasicQueryCompareOperation.CONTAINS.compare(
						dataSet.getMinValuesPerDimension()[i],
						dataSet.getMaxValuesPerDimension()[i],
						query[i].getMin(),
						query[i].getMax()))
					return false;
			}
			return true;
		}
	}

	private class InsertionIdOp implements
			DoOp
	{
		NumericIndexStrategy strategy;
		List<Region> regions;

		public InsertionIdOp(
				NumericIndexStrategy strategy,
				List<Region> regions ) {
			super();
			this.strategy = strategy;
			this.regions = regions;
		}

		public void putInRegions(
				List<ByteArrayId> ids,
				BasicNumericDataset dataSet ) {
			for (Region region : regions) {
				if (region.contains(
						dataSet)) {
					region.add(
							dataSet,
							ids);
				}
			}
		}

		public void run(
				NumericData[] set ) {
			BasicNumericDataset dataSet = new BasicNumericDataset(
					Arrays.copyOf(
							set,
							set.length));
			putInRegions(
					strategy.getInsertionIds(
							dataSet),
					dataSet);
		}
	}

	private static class ResultPair
	{
		NumericData[] query;
		int fn, fp, tn, tp;
		double precision;
		double recall;
		double accuracy;

		public ResultPair(
				NumericData[] query,
				int tp,
				int tn,
				int fp,
				int fn ) {
			super();
			this.query = query;
			this.fn = fn;
			this.fp = fp;
			this.tn = tn;
			this.tp = tp;
			this.precision = (double) tp / ((double) tp + (double) fp);
			this.recall = (double) tp / ((double) tp + (double) fn);
			this.accuracy = (double) (tp + tn) / (double) (tp + tn + fp + fn);
		}

		@Override
		public String toString() {
			return "\"" + Arrays.toString(
					query) + "\"," + fn + "," + fp + "," + tn + "," + tp + "," + precision + "," + recall + "," + accuracy;
		}

	}

	private class RegionBuilderOp implements
			DoOp
	{
		List<Region> results;

		public RegionBuilderOp(
				List<Region> results ) {
			super();
			this.results = results;
		}

		public void run(
				NumericData[] set ) {
			this.results.add(
					new Region(
							set));
		}

	}

	public void runOp(
			NumericIndexStrategy strategy,
			NumericData[] set,
			int d,
			double adjustment,
			int[] sizes,
			DoOp op ) {
		if (d == strategy.getOrderedDimensionDefinitions().length) {
			op.run(
					set);
		}
		else {
			final double increment = (strategy.getOrderedDimensionDefinitions()[d].getFullRange().getMax() - strategy.getOrderedDimensionDefinitions()[d].getFullRange().getMin()) / (double) sizes[d];
			for (int i = 0; i < sizes[d]; i++) {
				double value = strategy.getOrderedDimensionDefinitions()[d].getFullRange().getMin() + increment * (double) i;
				set[d] = new NumericRange(
						value,
						value + increment - (increment * adjustment));
				runOp(
						strategy,
						set,
						d + 1,
						adjustment,
						sizes,
						op);
			}
		}

	}

	public void populateIds(
			NumericIndexStrategy strategy,
			int sizes[],
			List<Region> regions ) {
		runOp(
				strategy,
				new NumericData[strategy.getOrderedDimensionDefinitions().length],
				0,
				1,
				sizes,
				new InsertionIdOp(
						strategy,
						regions));
	}

	@Test
	public void test() {
		testStrategy(
				sfcIndexStrategy);
	}

	public List<Region> buildRegions(
			NumericIndexStrategy strategy ) {
		List<Region> regions = new ArrayList<Region>();
		for (int x = 1; x < 10; x++) {
			for (int y = 2; y < 10; y++) {
				RegionBuilderOp op = new RegionBuilderOp(
						regions);
				runOp(
						strategy,
						new NumericData[strategy.getOrderedDimensionDefinitions().length],
						0,
						0.000001,
						new int[] {
							x,
							y
				},
						op);

			}
		}
		return regions;
	}

	public static class WorkerThread implements
			Runnable
	{

		AtomicInteger workIdAssigner;
		List<Region> regions;
		PrintWriter pw;
		NumericIndexStrategy strategy;

		public WorkerThread(
				AtomicInteger workIdAssigner,
				List<Region> regions,
				PrintWriter pw,
				NumericIndexStrategy strategy ) {
			super();
			this.workIdAssigner = workIdAssigner;
			this.regions = regions;
			this.pw = pw;
			this.strategy = strategy;
		}

		@Override
		public void run() {
			while (true) {
				int worktoDo = workIdAssigner.incrementAndGet() - 1;
		//		System.out.println(worktoDo);
				if (worktoDo >= regions.size()) return;
				Region region = regions.get(
						worktoDo);
				List<ByteArrayRange> queryRanges = strategy.getQueryRanges(
						new BasicNumericDataset(
								region.query));
				synchronized (pw) {
					pw.println(
							region.getResults(
									strategy,
									queryRanges));
					pw.flush();
				}

			}
		}

	}

	public void scheduleWork(
			NumericIndexStrategy strategy,
			List<Region> regions,
			PrintWriter pw ) {

		AtomicInteger workIdAssigner = new AtomicInteger(
				0);
		ExecutorService executor = Executors.newFixedThreadPool(
				5);
		for (int i = 0; i < 10; i++) {
			Runnable worker = new WorkerThread(
					workIdAssigner,
					regions,
					pw,
					strategy);
			executor.execute(
					worker);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {}
	}

	public void testStrategy(
			NumericIndexStrategy strategy ) {
		int sizes[] = new int[] {
			100,
			100
		};
		List<Region> regions = buildRegions(
				strategy);
		populateIds(
				strategy,
				sizes,
				regions);
		try (FileWriter wr = new FileWriter(
				"run.csv")) {
			java.io.PrintWriter po = new java.io.PrintWriter(
					wr);
			scheduleWork(
					strategy,
					regions,
					po);

		}
		catch (IOException e) {
			e.printStackTrace();
		}

	}

}
