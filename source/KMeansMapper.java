package kmeans;

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

/**
  * Mapper class for the k-Means Clustering.
  */
public class KMeansMapper extends Mapper<LongWritable, Text, DataPoint, DataPoint>
{
	/**
	  * List to hold k-Means Centroids.
	  */
	ArrayList<DataPoint> kCentroids;

	/**
	  * Overridden setup method of Mapper class.
	  * @param context
	  * Reads the file containing k-Means Centroids, parses it and loads the Centroids into the ArrayList kCentroids.
	  * Reads the file containing canopy centers, parses it and loads the Canopy Centers into the ArrayList canopyCenters.
	  * Creates a HashMap (Canopy Center, List of Centroids in this canopy).
	  */
	@Override
	public void setup(Context context)
		throws IOException, InterruptedException
	{
		// Call setup of super class
		super.setup(context);

		// Allocate memory for kCentroids list
		kCentroids = new ArrayList<DataPoint>();

		// Get the context's configuration
		Configuration configuration = context.getConfiguration();

		// Get a handle of the HDFS
		FileSystem filesystem = FileSystem.get(configuration);
		
		// Read the k-Means Centroid File
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

	/**
	  * Overridden map function of Mapper Class
	  * @param key
	  * - an offset in the file
	  * @param value
	  * - A tab separated String for each DataPoint
	  *	@param context
	  * <b>Outputs:</b><br>
	  * (key, value) pairs where,<br>
	  *	key is a Cluster Centroid associated with the current Data Point<br>
	  *	value is the Data Point being considered<br><br>
	  * 
	  * The function receives a (key, value) pair, parses it into the Data Point.
	  * For each DataPoint, we copute its distance to each k-Means Centroid
	  * The pair (K-Means Centroid, DataPoint) with the minimum distance is written as output.
	  */
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException
	{
		// Convert the value from Text to String 
		DataPoint dataPoint = new DataPoint(value.toString());

		// Set the minimum distance to the maximum value a double can hold
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
