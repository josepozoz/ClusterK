package source map-reduce;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import datapoint.DataPoint;


public class KMeansDriver
{
	
	public static ArrayList<DataPoint> oldKCentroids, newKCentroids;


	public static final String partFile = "/part-r-00000";


	
	public static boolean hasConverged(String oldKCentroidsFile, String newKCentroidsFile)
		throws Exception
	{
		// String to hold lines read from files
		String line;

		// Allocate memory for oldKCentroids and newKCentroids
		oldKCentroids = new ArrayList<DataPoint>();
		newKCentroids = new ArrayList<DataPoint>();

		// Create a new Configuration and obtain a handle on the FileSystem
		Configuration configuration = new Configuration();
		FileSystem filesystem = FileSystem.get(configuration);

		// Read the Old k-Means Centroid File
		if(DataPoint.NUM_ITERATIONS == 0)
		{
			// If this is the first iteration, the centroids lie in the k-Means Centroid input file
			BufferedReader oldReader = new BufferedReader(new InputStreamReader(filesystem.open
				(new Path(configuration.get("fs.default.name") + oldKCentroidsFile))));

			line = oldReader.readLine();
			while(line != null)
			{
				oldKCentroids.add(new DataPoint(line));
				line = oldReader.readLine();
			}
		}
		else
		{
			
			BufferedReader oldReader = new BufferedReader(new InputStreamReader(filesystem.open
				(new Path(configuration.get("fs.default.name") + oldKCentroidsFile + partFile))));
			
			line = oldReader.readLine();
			while(line != null)
			{
				
				int tabPosition = line.indexOf("\t");
				oldKCentroids.add(new DataPoint(line.substring(tabPosition + 1)));
				line = oldReader.readLine();
			}
		}

	
		BufferedReader newReader = new BufferedReader(new InputStreamReader(filesystem.open(
			new Path(configuration.get("fs.default.name") + newKCentroidsFile + partFile))));
		line = newReader.readLine();
		while(line != null)
		{
		
			int tabPosition = line.indexOf("\t");
			newKCentroids.add(new DataPoint(line.substring(tabPosition + 1)));
			line = newReader.readLine();
		}

	
		for(int i = 0; i < oldKCentroids.size(); i++)
		{
			if(oldKCentroids.get(i).complexDistance(newKCentroids.get(i)) > DataPoint.CONVERGENCE_THRESHOLD)
				return false;
		}
		return true;
	}


	public static void copyFinalCentroidsFile(String outputFolderName)
		throws Exception
	{
	
		Configuration configuration = new Configuration();
		FileSystem filesystem = FileSystem.get(configuration);

		
		FileUtil.copy(filesystem, new Path(configuration.get("fs.default.name") + outputFolderName + "_" + (DataPoint.NUM_ITERATIONS-1)), filesystem, 
			new Path(configuration.get("fs.default.name") + outputFolderName), false, true, configuration);
	}


	public static void main(String[] args)
		throws Exception
	{
	
		if(args.length != 3)
		{
			System.out.println("Usage: kMeansDriver <Input Path> <K-Centroids File> <Output Path>");
			System.exit(-1);
		}

		while(true)
		{
			
			Configuration configuration = new Configuration();
			if(DataPoint.NUM_ITERATIONS == 0)
				configuration.set("kCentroidsFile", args[1]);
			else
				configuration.set("kCentroidsFile", args[2] + "_" + (DataPoint.NUM_ITERATIONS-1) + partFile);

			System.out.println("Iteration: " + DataPoint.NUM_ITERATIONS);
			configuration.set("iteration", ""+DataPoint.NUM_ITERATIONS);

		
			Job job = new Job(configuration);
			job.setJarByClass(KMeansDriver.class);
			job.setJobName("K-Means Clustering");
		
		
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[2] + "_" + DataPoint.NUM_ITERATIONS));
		
		
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
		
			
			job.setMapOutputKeyClass(DataPoint.class);
			job.setMapOutputValueClass(DataPoint.class);
		
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DataPoint.class);
	
			job.waitForCompletion(true);

			
			if(DataPoint.NUM_ITERATIONS == 0)
			{
				if(hasConverged(args[1], args[2] + "_" + DataPoint.NUM_ITERATIONS))
					break;
			}
			else
			{
				if(hasConverged(args[2] + "_" + (DataPoint.NUM_ITERATIONS - 1), args[2] + "_" + DataPoint.NUM_ITERATIONS))
					break;
			}

			DataPoint.NUM_ITERATIONS++;
		}

		
		copyFinalCentroidsFile(args[2]);
	}
}
