Map
package de.jungblut.clustering.mapreduce;
	
	import java.io.IOException;
	import java.util.ArrayList;
	import java.util.List;
	
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.SequenceFile;
	import org.apache.hadoop.mapreduce.Mapper;
	
	import de.jungblut.clustering.model.ClusterCenter;
	import de.jungblut.clustering.model.VectorWritable;
	import de.jungblut.distance.DistanceMeasurer;
	import de.jungblut.distance.ManhattanDistance;
	
	// first iteration, k-random centers, in every follow-up iteration we have new calculated centers
	public class KMeansMapper extends Mapper<ClusterCenter, VectorWritable, ClusterCenter, VectorWritable> {
	
		private final List<ClusterCenter> centers = new ArrayList<>();
		private DistanceMeasurer distanceMeasurer;
	
		@SuppressWarnings("deprecation")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path centroids = new Path(conf.get("centroid.path"));
			FileSystem fs = FileSystem.get(conf);
	
			try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf)) {
				ClusterCenter key = new ClusterCenter();
				IntWritable value = new IntWritable();
				int index = 0;
				while (reader.next(key, value)) {
					ClusterCenter clusterCenter = new ClusterCenter(key);
					clusterCenter.setClusterIndex(index++);
					centers.add(clusterCenter);
				}
			}
			distanceMeasurer = new ManhattanDistance();
		}
	
		@Override
		protected void map(ClusterCenter key, VectorWritable value, Context context) throws IOException,
				InterruptedException {
	
			ClusterCenter nearest = null;
			double nearestDistance = Double.MAX_VALUE;
			for (ClusterCenter c : centers) {
				double dist = distanceMeasurer.measureDistance(c.getCenterVector(), value.getVector());
				if (nearest == null) {
					nearest = c;
					nearestDistance = dist;
				} else {
					if (nearestDistance > dist) {
						nearest = c;
						nearestDistance = dist;
					}
				}
			}
			context.write(nearest, value);
		}
	
	}


Reduce
package de.jungblut.clustering.mapreduce;
	
	import java.io.IOException;
	import java.util.ArrayList;
	import java.util.List;
	
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.SequenceFile;
	import org.apache.hadoop.mapreduce.Reducer;
	
	import de.jungblut.clustering.model.ClusterCenter;
	import de.jungblut.clustering.model.VectorWritable;
	import de.jungblut.math.DoubleVector;
	
	// calculate a new clustercenter for these vertices
	public class KMeansReducer extends Reducer<ClusterCenter, VectorWritable, ClusterCenter, VectorWritable> {
	
		public static enum Counter {
			CONVERGED
		}
	
		private final List<ClusterCenter> centers = new ArrayList<>();
	
		@Override
		protected void reduce(ClusterCenter key, Iterable<VectorWritable> values, Context context) throws IOException,
				InterruptedException {
	
			List<VectorWritable> vectorList = new ArrayList<>();
			DoubleVector newCenter = null;
			for (VectorWritable value : values) {
				vectorList.add(new VectorWritable(value));
				if (newCenter == null)
					newCenter = value.getVector().deepCopy();
				else
					newCenter = newCenter.add(value.getVector());
			}
	
			newCenter = newCenter.divide(vectorList.size());
			ClusterCenter center = new ClusterCenter(newCenter);
			centers.add(center);
			for (VectorWritable vector : vectorList) {
				context.write(center, vector);
			}
	
			if (center.converged(key))
				context.getCounter(Counter.CONVERGED).increment(1);
	
		}
	
		@SuppressWarnings("deprecation")
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			Configuration conf = context.getConfiguration();
			Path outPath = new Path(conf.get("centroid.path"));
			FileSystem fs = FileSystem.get(conf);
			fs.delete(outPath, true);
			try (SequenceFile.Writer out = SequenceFile.createWriter(fs, context.getConfiguration(), outPath,
					ClusterCenter.class, IntWritable.class)) {
				final IntWritable value = new IntWritable(0);
				for (ClusterCenter center : centers) {
					out.append(center, value);
				}
			}
		}
	}
