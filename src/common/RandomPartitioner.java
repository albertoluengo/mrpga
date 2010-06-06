package common;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * Clase que sobreescribe el comportamiento de la clase 
 * <code>Partitioner</code> por defecto para sustituirlo por otro que devuelva
 * un numero aleatorio entero de particiones independientes del par <clave,valor>
 * pasados.
 * @author Alberto Luengo Cabanillas
 */
public class RandomPartitioner extends Partitioner<Text,IntWritable>{

	Random rng;

	/**
	 * Método constructor de la clase<code>RandomPartitioner</code> que se ocupa
	 * de generar una nueva semilla para números aleatorios.
	 */
	RandomPartitioner() {
		rng = new Random(System.nanoTime());
	}
	
	@Override
	public int getPartition(Text key, IntWritable value, int numReducers) {
		return (Math.abs(rng.nextInt()) % numReducers);
	}
}

