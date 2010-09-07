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
public class TargetPhraseMapper extends MRPGAMapper {
	
	public TargetPhraseMapper(){
		super();
	}
	
	@Override
	public void problemSetup(Hashtable configParams, Hashtable mappersParams) {
	}
	

	/**
	 * M&#233;todo que calcula el "fitness" de cada individuo. En el caso del problema
	 * <code>TargetPhrase</code> consistir&#225; en incrementarlo en funci&#243;n de la
	 * diferencia entre cada uno de los caracteres del individuo y de la frase
	 * objetivo.
	 * @param individual Individuo a procesar
	 * @return Valor num&#233;rico con precisi&#243;n <code>double</code> que representa el fitness del individuo.
	 */
	public DoubleWritable calculateFitness(Hashtable problemParams, Hashtable generalParams, Text individual) {
		String targetPhrase =(String)problemParams.get("targetPhrase");
		int targetSize=targetPhrase.length();
		String textAsString = individual.toString();
		int fitness=0;
		for (int j=0; j<targetSize; j++) {
			fitness += Math.abs((textAsString.toCharArray()[j] - targetPhrase.charAt(j)));
		}
		return new DoubleWritable(fitness);
	}
}
