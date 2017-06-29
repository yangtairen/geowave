package mil.nga.giat.geowave.analytic.javaspark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;

public class KMeansHullGenerator
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			KMeansHullGenerator.class);

	public static JavaPairRDD<Integer, Geometry> generateHullsRDD(
			final JavaRDD<Vector> inputPoints,
			final KMeansModel clusterModel ) {

		// Group the input points by their kmeans centroid index
		final JavaPairRDD<Integer, Iterable<Vector>> pointGroupRDD = inputPoints.groupBy(
				new Function<Vector, Integer>() {
					/**
					 *
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(
							final Vector point )
							throws Exception {
						return clusterModel.predict(
								point);
					}
				});

		LOGGER.warn(
				"GroupBy RDD has " + pointGroupRDD.count() + " rows");

		// Create the convex hull for each kmeans centroid
		final JavaPairRDD<Integer, Geometry> hullRDD = pointGroupRDD.mapValues(
				new Function<Iterable<Vector>, Geometry>() {

					/**
					 *
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Geometry call(
							final Iterable<Vector> it )
							throws Exception {
						final Iterable<Coordinate> coordIt = Iterables.transform(it, new com.google.common.base.Function<Vector, Coordinate>() {

							@Override
							public Coordinate apply(
									final Vector input ) {
								return new Coordinate(
											input.apply(
													0),
											input.apply(
													1));
							}});
						final Coordinate[] coordArray = Iterables.toArray(coordIt, Coordinate.class);
						return new ConvexHull(coordArray,GeometryUtils.GEOMETRY_FACTORY).getConvexHull();
					}

				});

		LOGGER.warn(
				"Hull geom RDD has " + hullRDD.count() + " rows");

		return hullRDD;
	}

	public static Geometry[] generateHullsLocal(
			final JavaRDD<Vector> inputCentroids,
			final KMeansModel clusterModel ) {
		final Geometry[] hulls = new Geometry[clusterModel.clusterCenters().length];

		final List<Vector> inputList = inputCentroids.collect();

		// Run each input through the model to get its centroid and create the
		// hull
		for (final Vector point : inputList) {
			final int centroidIndex = clusterModel.predict(
					point);

			if (hulls[centroidIndex] == null) {
				hulls[centroidIndex] = GeometryUtils.GEOMETRY_FACTORY.buildGeometry(
						Collections.EMPTY_LIST);
			}

			final Coordinate coord = new Coordinate(
					point.apply(
							0),
					point.apply(
							1));

			final Geometry union = hulls[centroidIndex].union(
					GeometryUtils.GEOMETRY_FACTORY.createPoint(
							coord));

			hulls[centroidIndex] = union.convexHull();
		}

		return hulls;
	}
}
