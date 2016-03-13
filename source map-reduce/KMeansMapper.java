package source map-reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import datapoint.DataPoint;


public class KMeansMapper extends Mapper<LongWritable, Text, DataPoint, DataPoint>
{

	ArrayList<DataPoint> kCentroids;

	
	@Override
	public void setup(Context context)
		throws IOException, InterruptedException
	{
	
		super.setup(context);

		
		kCentroids = new ArrayList<DataPoint>();

		
		Configuration configuration = context.getConfiguration();

		
		FileSystem filesystem = FileSystem.get(configuration);
		
	
		BufferedReader centroidReader = new BufferedReader(new InputStreamReader(filesystem.open(
			new Path(configuration.get("fs.default.name") + configuration.get("kCentroidsFile")))));
		String line = centroidReader.readLine();
		while(line != null)
		{
			DataPoint kCentroid;
			if(configuration.get("iteration").equals("0"))
				kCentroid = new DataPoint(line);
			else
			{
				int tabPosition = line.indexOf("\t");
				kCentroid = new DataPoint(line.substring(tabPosition + 1));
			}
			kCentroids.add(kCentroid);
			line = centroidReader.readLine();
		}
		centroidReader.close();
	}

	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException
	{
	
		DataPoint dataPoint = new DataPoint(value.toString());

		
		double minDistance = Double.MAX_VALUE;
		int offset = -1;

		for(int i = 0; i < kCentroids.size(); i++)
		{
			DataPoint centroid = kCentroids.get(i);
			double distance = dataPoint.complexDistance(centroid);
			// Check if the distance is less than the minimum distance found so far
			if(distance < minDistance)
			{
				minDistance = distance;
				offset = i;
			}
		}
		context.write(kCentroids.get(offset), dataPoint);
	}
}
