package common;

import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Clase que sobreescribe el comportamiento de la clase 
 * <code>Partitioner</code> por defecto para sustituirlo por otro que devuelva
 * un número aleatorio entero de particiones independientes del par (clave,valor)
 * pasados.
 * @author Alberto Luengo Cabanillas
 */
public class RandomPartitioner extends Partitioner<Text,IntWritable>{

	Random rng;

	/**
	 * M&#233;todo constructor de la clase<code>RandomPartitioner</code> que se ocupa
	 * de generar una nueva semilla para n&#250;meros aleatorios.
	 */
	RandomPartitioner() {
		rng = new Random(System.nanoTime());
	}
	
	/**
	 * M&#233;todo que devuelve una partici&#243;n de datos sobre la que trabajar&#225;	 el nodo
	 * <code>Reducer</code> correspondiente.
	 * @param key Clave del individuo en formato <code>Text</code>.
	 * @param value Fitness del individuo en formato num&#233;rico entero.
	 * @param numReducers N&#250;mero entero de nodos <code>Reducer</code> en el trabajo <code>MapReduce</code>.
	 * @return N&#250;mero entero de la partici&#243;n sobre la que actuar&#225;n los nodos <code>Reducer</code>.
	 */
	@Override
	public int getPartition(Text key, IntWritable value, int numReducers) {
		return (Math.abs(rng.nextInt()) % numReducers);
	}
}

