package common;

import java.util.Random;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Clase que sobreescribe el comportamiento de la clase 
 * <code>Partitioner</code> por defecto para sustituirlo por otro que devuelva
 * un numero aleatorio de precisi&#243;n <code>Double</code> independientes del 
 * par (clave,valor) pasados.
 * @author Alberto Luengo Cabanillas
 */
public class RandomDoublePartitioner extends Partitioner<Text,DoubleWritable>{
	
	/**
	 * M&#233;todo que obtiene un n&#250;mero <code>double</code> en un rango determinado.
	 * @param r Instancia de la clase <code>Random</code> para generar n&#250;meros aleatorios.
	 * @param lower N&#250;mero entero que representa el rango inferior de la b&#250;squeda.
	 * @param higher N&#250;mero entero que representa el rango superior de la b&#250;squeda.
	 * @return N&#250;mero entero aleatorio dentro del rango especificado.
	 */
	public int randomInRange(Random r, int lower, int higher) { 
	 int ran = r.nextInt();
	 double x = (double)ran/Integer.MAX_VALUE * (higher-lower);
	 return (int)x + lower;
	}
	
	/**
	 * M&#233;todo que devuelve una partici&#243;n de datos sobre la que trabajar&#225;	 el nodo
	 * <code>Reducer</code> correspondiente.
	 * @param key Clave del individuo en formato <code>Text</code>.
	 * @param value Fitness del individuo en formato num&#233;rico entero.
	 * @param numPartitions N&#250;mero entero de particiones.
	 * @return N&#250;mero entero de la partici&#243;n sobre la que actuar&#225;n los nodos <code>Reducer</code>.
	 */
	@Override
	public int getPartition(Text key, DoubleWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		Random rand = new Random();
		return randomInRange(rand, 0, numPartitions-1);
	}
}
