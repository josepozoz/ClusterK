package source map-reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import datapoint.DataPoint;


public class KMeansReducer extends Reducer<DataPoint, DataPoint, IntWritable, DataPoint>
{

	public static int CENTROID_KEY = 1;

	@Override
	public void reduce(DataPoint key, Iterable<DataPoint> values, Context context)
		throws IOException, InterruptedException
	{
		
		DataPoint dataPoint = DataPoint.getAverageDataPoint(values);
		context.write(new IntWritable(KMeansReducer.CENTROID_KEY), dataPoint);

		
		KMeansReducer.CENTROID_KEY++;
	}
}
