package source;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import datapoint.DataPoint;


public class ClusterAssignReducer extends Reducer<DataPoint, DataPoint, DataPoint, DataPoint>
{
	
	@Override
	public void reduce(DataPoint key, Iterable<DataPoint> values, Context context)
		throws IOException, InterruptedException
	{
		for(DataPoint dataPoint : values)
			context.write(key, dataPoint);
	}
}
