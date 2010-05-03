package common;

import java.util.Random;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Sobreescribimos el Partitioner por defecto para sustituirlo por otro que devuelva
 * un numero aleatorio entero de distribucion uniforme
 */
public class RandomDoublePartitioner extends Partitioner<Text,DoubleWritable>{
	
	//Metodo que nos devuelve un numero aleatorio en un rango determinado
	public int randomInRange(Random r, int lower, int higher) { 
	 int ran = r.nextInt();
	 double x = (double)ran/Integer.MAX_VALUE * (higher-lower);
	 return (int)x + lower;
	}
	
	@Override
	public int getPartition(Text key, DoubleWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		Random rand = new Random();
		return randomInRange(rand, 0, numPartitions-1);
	}
}
