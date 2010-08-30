package problems;

import java.util.Hashtable;
import java.util.Random;

import common.MRPGAReducer;

/**
 * Clase que implementa todas las funciones necesarias de un nodo <code>Mapper</code> 
 * en un trabajo <code>MapReduce</code>: se encargar&#225; de evaluar el "fitness" de cada individuo,
 * as&#237; como de generar los distintos pares <clave, fitness> necesarios para que los
 * nodos <code>Reducer</code> los puedan procesar. 
 * @author Alberto Luengo Cabanillas
 */
public class TargetPhraseReducer extends MRPGAReducer {
	
	private Random r;
	
	public TargetPhraseReducer(){
		super();
	}

	@Override
	public void problemSetup(Hashtable configParams, Hashtable mappersParams) {
		r = new Random(System.nanoTime());
	}
	
	//Cruce de doble punto (DPX)...
	@Override
	public String[][] crossOver(Hashtable configParams, Hashtable generalParams, String[][]crossArray) {
		int tournamentSize =Integer.parseInt((String)configParams.get("tournWindow"));
		String[][] newIndividuals = new String[crossArray.length][tournamentSize];
		
		String[] parent1 = crossArray[0];
		String[] parent2 = crossArray[1];
		
		//Establecemos los puntos de corte para ver como se generan los descendientes
		int cutPoint1 = (int) ((Math.random()*(parent1[0].length()- 2))+ 1);
		int cutPoint2 = (int) ((Math.random()*(parent2[0].length()- 2))+ 1);
		while (cutPoint1 >= cutPoint2){
			cutPoint2 = (int) ((Math.random()*(parent2[0].length()- 1))+ 1);
		}
		
		//Creamos las partes identicas a las de los padres...
		String[] child1 = new String[parent1.length];
		String[] child2 = new String[parent2.length];
		
		for (int aux = 0; aux<parent1.length;aux++) {
//			LOG.info("PARENT1["+aux+"] VALE "+parent1[aux]);
//			LOG.info("PARENT2["+aux+"] VALE "+parent2[aux]);
			String p1 = parent1[aux];
			String p2 = parent2[aux];
			String frag1A = p1.substring(0, cutPoint1+1);
			String frag2A = p1.substring(cutPoint1+1, cutPoint2+1);
			String frag3A = p1.substring(cutPoint2+1, p1.length());
			
			String frag1B = p2.substring(0, cutPoint1+1);
			String frag2B = p2.substring(cutPoint1+1, cutPoint2+1);
			String frag3B = p2.substring(cutPoint2+1, p2.length());
			
			//Concatenamos...
			child1[aux] = frag1B+frag2A+frag3B;
//			LOG.info("CHILD1["+aux+"] VALE "+child1[aux]);
			child2[aux] = frag1A+frag2B+frag3A;
//			LOG.info("CHILD2["+aux+"] VALE "+child2[aux]);
			}
		//Creamos un nuevo array de arrays...
		newIndividuals[0] = child1;
		newIndividuals[1] = child2;
		
		return newIndividuals;
		}

	@Override
	public String mutate(Hashtable configParams, Hashtable generalParams,String individual) {
		double mutationRate = Double.parseDouble((String)configParams.get("mutationRate"));
		double random = r.nextDouble();
		String sText = individual.toString();
		String mutInd = "";
		int beginIndex = 0, endIndex = 0;
		
		//Si el numero aleatorio cae dentro del rango de mutacion, seguimos...
		if (random < mutationRate) {
			//Obtenemos dos posiciones aleatorias dentro del Individuo...
			int r1 = (int) ((Math.random()*(sText.length()- 1))+ 1);
			int r2 = (int) ((Math.random()*(sText.length()- 1))+ 1);
			
			if (r1 == r2) {
				mutInd = sText;
			}
			else {
				if (r1 < r2) {
					beginIndex = r1;
					endIndex = r2;
				}
				else {
					beginIndex = r2;
					endIndex = r1;
				}
				//Obtenemos los genes que se encuentran en esas posiciones...
				char g1 = sText.charAt(r1);
				char g2 = sText.charAt(r2);
				
				//Intercambiamos las posiciones de esos genes...
				mutInd = sText.substring(0,beginIndex);
				mutInd = mutInd.concat(g2+"").concat(sText.substring(beginIndex+1,endIndex)).concat(g1+"").concat(sText.substring(endIndex+1, sText.length()));
			}
		}
		//...si no, devolvemos el individuo tal cual...
		else {
			mutInd = sText;
		}
		return mutInd;
	}
}
