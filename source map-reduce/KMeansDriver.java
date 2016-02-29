package kmeans;

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

/**
  * Driver class for the package. Initializes the MapReduce job to find stable K-Means Centroids.
  */
public class KMeansDriver
{
	/**
	  * ArrayLists holding k-Means Centriods before and after an iteration. Used to check convergence.
	  */
	public static ArrayList<DataPoint> oldKCentroids, newKCentroids;

	/**
	  * Name of the output part file.
	  */
	public static final String partFile = "/part-r-00000";


	/**
	  * Checks if k-Means Centroids have converged
	  * @param oldKCentroidsFile
	  * - String path to the old k-Means Centroid File.
	  * @param newKCentroidsFile
	  * - String path to the new k-Means Centroid File.
	  * Converts the String file names to Path objects.
	  * Then, the files are read from the FileSystem and parsed.
	  * The oldKCentroids and newKCentroids are populated and checked if they are within the defined thresholds.
	  */
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
			// If this is not the first iteration, the centroids lie in a part file
			BufferedReader oldReader = new BufferedReader(new InputStreamReader(filesystem.open
				(new Path(configuration.get("fs.default.name") + oldKCentroidsFile + partFile))));
			
			line = oldReader.readLine();
			while(line != null)
			{
				// Since the output is a (key, value) pair, the key is ignored
				int tabPosition = line.indexOf("\t");
				oldKCentroids.add(new DataPoint(line.substring(tabPosition + 1)));
				line = oldReader.readLine();
			}
		}

		// Read the New k-Means Centroid File
		BufferedReader newReader = new BufferedReader(new InputStreamReader(filesystem.open(
			new Path(configuration.get("fs.default.name") + newKCentroidsFile + partFile))));
		line = newReader.readLine();
		while(line != null)
		{
			// Since the file contains a (key, value) pair, the key is ignored
			int tabPosition = line.indexOf("\t");
			newKCentroids.add(new DataPoint(line.substring(tabPosition + 1)));
			line = newReader.readLine();
		}

		// Check if the corresponding Old and New Centroids have converged
		for(int i = 0; i < oldKCentroids.size(); i++)
		{
			if(oldKCentroids.get(i).complexDistance(newKCentroids.get(i)) > DataPoint.CONVERGENCE_THRESHOLD)
				return false;
		}
		return true;
	}

	/**
	  * Copies the final k-Means Centroids file to a the folder given by the parameter
	  * @param outputFolderName
	  * - Name of the output folder.
	  * Uses the the copy method of FileUtil class to copy the converged centroids into the output path given.
	  */
	public static void copyFinalCentroidsFile(String outputFolderName)
		throws Exception
	{
		// Create a new Configuration and get a handle on the File System
		Configuration configuration = new Configuration();
		FileSystem filesystem = FileSystem.get(configuration);

		// Use the copy method of FileUtil to copy the final k-Means Centroid file to a known output file
		FileUtil.copy(filesystem, new Path(configuration.get("fs.default.name") + outputFolderName + "_" + (DataPoint.NUM_ITERATIONS-1)), filesystem, 
			new Path(configuration.get("fs.default.name") + outputFolderName), false, true, configuration);
	}

	/** 
	  * Main function
	  * @param args
	  * @param args[0]
	  * - Path to file containing the Data Set.
	  * @param args[1]
	  * - Path to file containing intital k Centroids.
	  * @param args[2]
	  * - Path to output file.
	  */
	public static void main(String[] args)
		throws Exception
	{
		// Check if a sufficient number of arguments are provided
		// args[0] = Path to file containing the Data Set
		// args[1] = Path to file containing intital k Centroids
		// args[2] = Path to output file
		if(args.length != 3)
		{
			System.out.println("Usage: kMeansDriver <Input Path> <K-Centroids File> <Output Path>");
			System.exit(-1);
		}

		while(true)
		{
			// Add parameter to the configuration - Path to k-Centroids files
			Configuration configuration = new Configuration();
			if(DataPoint.NUM_ITERATIONS == 0)
				configuration.set("kCentroidsFile", args[1]);
			else
				configuration.set("kCentroidsFile", args[2] + "_" + (DataPoint.NUM_ITERATIONS-1) + partFile);

			System.out.println("Iteration: " + DataPoint.NUM_ITERATIONS);
			configuration.set("iteration", ""+DataPoint.NUM_ITERATIONS);

			// Set up the job with the configuration defined above
			Job job = new Job(configuration);
			job.setJarByClass(KMeansDriver.class);
			job.setJobName("K-Means Clustering");
		
			// Set paths for input and output files
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[2] + "_" + DataPoint.NUM_ITERATIONS));
		
			// Set the Mapper and Reducer class
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
		
			// Specify the class types of the key and value produced by the mapper
			job.setMapOutputKeyClass(DataPoint.class);
			job.setMapOutputValueClass(DataPoint.class);
		
			// Specify the class types of the key and value produced by the reducer
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DataPoint.class);
	
			job.waitForCompletion(true);

			// Check if the k-Means Centroids have converged
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

		// Copy the final k-Means Centroids File to a fixed location
		copyFinalCentroidsFile(args[2]);
	}
}
