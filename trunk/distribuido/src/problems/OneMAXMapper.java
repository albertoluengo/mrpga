package problems;

import java.util.Hashtable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import common.MRPGAMapper;

/**
 * Clase que implementa todas las funciones necesarias de un nodo <code>Mapper</code> 
 * en un trabajo <code>MapReduce</code>: se encargar&#225; de evaluar el "fitness" de cada individuo,
 * as&#237; como de generar los distintos pares <clave, fitness> necesarios para que los
 * nodos <code>Reducer</code> los puedan procesar. 
 * @author Alberto Luengo Cabanillas
 */
public class OneMAXMapper extends MRPGAMapper {
	
	public OneMAXMapper(){
		super();
	}
	
	@Override
	public void problemSetup(Hashtable configParams, Hashtable mappersParams) {}

	/**
	 * M&#233;todo que calcula el "fitness" de cada individuo.
	 * @param individual Individuo a procesar
	 * @return Valor num&#233;rico con precisi&#243;n <code>double</code> que representa el fitness del individuo.
	 */
	public DoubleWritable calculateFitness(Hashtable configParams, Hashtable mappersParams, Text individual) {
		double fitness = 0.0;
		for (int i=0; i<individual.getLength(); i++) {
			if (individual.charAt(i)=='1')
				fitness += 1.0;
		}
		return new DoubleWritable(fitness);
	}
}
