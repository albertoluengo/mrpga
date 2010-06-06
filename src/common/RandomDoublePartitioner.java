package common;

import java.util.Random;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Clase que sobreescribe el comportamiento de la clase 
 * <code>Partitioner</code> por defecto para sustituirlo por otro que devuelva
 * un numero aleatorio de precisión <code>Double</code> independientes del 
 * par <clave,valor> pasados.
 * @author Alberto Luengo Cabanillas
 */
public class RandomDoublePartitioner extends Partitioner<Text,DoubleWritable>{
	
	//Metodo que nos devuelve un numero aleatorio en un rango determinado
	/**
	 * Método que obtiene un número <code>double</code> en un rango determinado.
	 * @param r Instancia de la clase <code>Random</code> para generar números aleatorios.
	 * @param lower Número entero que representa el rango inferior de la búsqueda.
	 * @param higher Número entero que representa el rango superior de la búsqueda.
	 */
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
