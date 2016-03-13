package datapoint;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.WritableComparable;


public class DataPoint implements WritableComparable<DataPoint>
{
	/**
	  * Attributes for the class: year and temperature.
	  */
	public int edad , alojamiento;

	/**
	  * T1 and T2 thresholds for this Data Set.
	  */
	public final static double T1 = 10, T2 = 5;
	/**
	  * Threshold for convergence. A distance value below the specified value denotes the point has converged.
	  */
	public final static double CONVERGENCE_THRESHOLD = 1.0;

	/**
	 * The number of iterations so far.
	 */
	public static long NUM_ITERATIONS = 0;

	
	public DataPoint()
	{
		year = temperature = 0;
	}

	
	public DataPoint(String dataPointString)
	{
		int commaPosition = dataPointString.indexOf(",");
		edad = Integer.parseInt(dataPointString.substring(0, commaPosition));
		alojamiento = Integer.parseInt(dataPointString.substring(commaPosition + 1));
	}

	
	public DataPoint(DataPoint dataPoint)
	{
		edad = dataPoint.year;
		alojamiento = dataPoint.temperature;
	}

	
	public void write(DataOutput out)
		throws IOException
	{
		out.writeInt(year);
		out.writeInt(temperature);
	}

	
	public void readFields(DataInput in)
		throws IOException
	{
		edad = in.readInt();
		alojamiento = in.readInt();
	}


	public DataPoint read(DataInput in)
		throws IOException
	{
		DataPoint dataPoint = new DataPoint();
		dataPoint.readFields(in);
		return dataPoint;	
	}


	public int compareTo(DataPoint dataPoint)
	{
		return (alojamiento < dataPoint.alojamiento ? -1 : 
			(alojamiento == dataPoint.alojamiento ? (edad < dataPoint.edad ? -1 : (edad == dataPoint.edad ? 0 : 1)) : 1));
	}


	public boolean withinT1(DataPoint dataPoint)
	{
		return (simpleDistance(dataPoint) < T1);
	}


	public boolean withinT2(DataPoint dataPoint)
	{
		return (simpleDistance(dataPoint) < T2);
	}

	
	public int simpleDistance(DataPoint dataPoint)
	{
		return Math.abs(alojamiento - dataPoint.alojamiento);
	}


	public double complexDistance(DataPoint dataPoint)
	{
		return Math.sqrt(Math.abs((edad - dataPoint.edad) * (edad - dataPoint.edad) 
				+ (alojamiento - dataPoint.alojamientoe) * (alojamiento - dataPoint.alojamiento)));
	}

	
	public String toString()
	{
		return edad + "," + alojamiento;
	}

	
	@Override
	public boolean equals(Object object)
	{
		if(object == null)
			return false;
		DataPoint dataPoint = (DataPoint) object;
		if(edad ==  dataPoint.edad && alojamiento == dataPoint.alojamiento)
			return true;
		return false;
	}

	
	@Override
	public int hashCode()
	{
		return (18 * year + 1 * alojamiento);
	}

	
	public static DataPoint getAverageDataPoint(Iterable<DataPoint> dataPoints)
	{
		double edadSum = 0;
		double alojamientoSum = 0;
		long count = 0;
		for(DataPoint dataPoint: dataPoints)
		{
			edadSum += dataPoint.edad;
			alojamientoSum += dataPoint.alojamiento;
			count++;
		}
		DataPoint averageDataPoint =  new DataPoint();
		averageDataPoint.edad = (int) (edadSum/count);
		averageDataPoint.alojamiento = (int) (alojamientoSum/count);
		return averageDataPoint;
	}
}
