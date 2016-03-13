package source;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import datapoint.DataPoint;

/**
  * clase Mapper para el paso Asignar Cluster
  */
public class ClusterAssignMapper extends Mapper<LongWritable, Text, DataPoint, DataPoint>
{
	
	public static ArrayList<DataPoint> kCentroids;

	
	@Override
	public void setup(Context context)
		throws IOException, InterruptedException
	{
	
		super.setup(context);

		// Asignar memoria para kCentroids
		kCentroids = new ArrayList<DataPoint>();

	
		Configuration configuration = context.getConfiguration();

		
		Path path = new Path(configuration.get("fs.default.name") + configuration.get("kCentroidsFile"));
		FileSystem filesystem = FileSystem.get(configuration);
		BufferedReader reader = new BufferedReader(new InputStreamReader(filesystem.open(path)));

		String line = reader.readLine();
		while(line != null)
		{
			DataPoint centroid =  new DataPoint(line);
			kCentroids.add(centroid);
			line = reader.readLine();
		}
	}

	
	@Override 
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException
	{
		
		DataPoint dataPoint = new DataPoint(value.toString());

	
		double minDistance = Double.MAX_VALUE;
		double distance;
		int offset = -1;

		
		for(int i = 0; i < kCentroids.size(); i++)
		{
			distance = dataPoint.complexDistance(kCentroids.get(i));
			if(distance < minDistance)
			{
				minDistance = distance;
				offset = i;
			}
		}

		
		context.write(kCentroids.get(offset), dataPoint);
	}
}
