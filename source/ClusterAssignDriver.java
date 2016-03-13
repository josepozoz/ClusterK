package source;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import datapoint.DataPoint;


public class ClusterAssignDriver
{
	public static void main(String args[])
		throws Exception
	{
		
		if(args.length != 3)
		{
			System.out.println("Usage: ClusterAssignDriver <Input Path> <k-Centroids File> <Output Path>");
			System.exit(-1);
		}

	
		Configuration configuration = new Configuration();
		configuration.set("kCentroidsFile", args[1]);

		
		Job job = new Job(configuration);
		job.setJarByClass(ClusterAssignDriver.class);
		job.setJobName("K-Means Clustering - Cluster Assignment");

		// Ajuste el Mapper y la clase Reductor
		job.setMapperClass(ClusterAssignMapper.class);
		job.setReducerClass(ClusterAssignReducer.class);

		// caminos definidos para archivos de entrada y salida
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// Especifica los tipos de clase de la clave y el valor producido por el asignador y el reductor .
		job.setOutputKeyClass(DataPoint.class);
		job.setOutputValueClass(DataPoint.class);

		System.exit(job.waitForCompletion(true)?0:1);
	}
}
