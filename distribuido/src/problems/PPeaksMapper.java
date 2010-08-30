package problems;

import java.util.Hashtable;
import java.util.Random;

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
public class PPeaksMapper extends MRPGAMapper {
	
	// Vector de picos
	private static short peak[][];
	int peaks_number, gene_length = 0;
	public PPeaksMapper(){
		super();
	}
	
	
	@Override
	public void problemSetup(Hashtable problemParams, Hashtable generalParams) {
		peaks_number = Integer.parseInt((String)problemParams.get("peaks_number"));
	    gene_length = Integer.parseInt((String)generalParams.get("gene_length"));
		peak = new short[peaks_number][gene_length];
		Random r = new Random(System.nanoTime());
		for(int peaks=0;peaks<peaks_number;peaks++)
		{
			for(int i=0;i<gene_length;i++)
				if(r.nextDouble()<0.5)	
					peak[peaks][i] = 1;
				else	
					peak[peaks][i] = 0;
		}
	}
	
	
	/**
	 * M&#233;todo que calcula el "fitness" de cada individuo.
	 * @param individual Individuo a procesar
	 * @param problemParams Tabla Hash con los parametros especificos del problema
	 * @param generalParams Tabla Hash con los parametros generales de los nodos Mapper
	 * @return Valor num&#233;rico con precisi&#243;n <code>double</code> que representa el fitness del individuo.
	 */
	public DoubleWritable calculateFitness(Hashtable problemParams, Hashtable generalParams, Text individual) {
		double fitness = 0.0;
	    //Bits en comun con el pico mas cercano
	    double nearest_peak = 999.0;
	    int i = 0, peaks = 0;
	    double currentDistance, distHamming = 0.0;
	    double []distances = new double[peaks_number];
	    
	    for(peaks=0; peaks<peaks_number; peaks++)
	    {
	      //...calculamos la distancia Hamming...
	      distHamming = 0.0;
	      for(int pos=0;pos<gene_length;pos++)
	      {
	    	  short current_peak = peak[peaks][pos];
    		  if(current_peak!=Integer.parseInt(individual.toString().charAt(pos)+""))
    			  distHamming++;
	      }
	      distances[peaks] = distHamming;
	    }
	    
	    //Buscamos ahora el valor mas pequeño...
	    for (i=0;i<distances.length;i++)
	    {
	    	currentDistance = distances[i];
	    	if (currentDistance < nearest_peak)
	    		nearest_peak = currentDistance;
	    }
	    fitness = (double)((double)nearest_peak / (double)individual.getLength());
		return new DoubleWritable(fitness);
	}
}
