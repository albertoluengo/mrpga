package common;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * Sobreescribimos el Partitioner por defecto para sustituirlo por otro que devuelva
 * un numero aleatorio entero independiente del par <K, V> suministrado...
 */
public class RandomPartitioner extends Partitioner<Text,IntWritable>{

	// Partitions randomly independent of the passed <K, V>
	Random rng;

	RandomPartitioner() {
		rng = new Random(System.nanoTime());
	}
	
//	@Override
//	public void setup(Context cont) throws IOException{
//		rng = new Random(System.nanoTime());
//	}
	
	@Override
	public int getPartition(Text key, IntWritable value, int numReducers) {
		return (Math.abs(rng.nextInt()) % numReducers);
	}
	
	//Metodo que nos devuelve un numero aleatorio en un rango determinado
//	public int randomInRange(Random r, int lower, int higher) { 
//	 int ran = r.nextInt();
//	 double x = (double)ran/Integer.MAX_VALUE * (higher-lower);
//	 return (int)x + lower;
//	}
//	
//	@Override
//	public int getPartition(Text key, IntWritable value, int numPartitions) {
//		// TODO Auto-generated method stub
//		Random rand = new Random();
//		return randomInRange(rand, 0, numPartitions-1);
//	}
}

