package fuentes;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.LineReader;



/**
 * Los nodos reducer ejecutaran la funcion reduce en funcion de las entradas que obtengan. 
 * En caso de que no este la carga balanceada, algunos
 * nodos reducer podran obtener los resultados intermedios de maquinas "vecinas". La funcion
 * "reduce", definida por el usuario, en este caso generara la descendencia...
 * 
 *
 */
public class HWorldPseudoReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	private IntWritable fitness = new IntWritable();
	private int crossSize = 2;
	private int tournamentSize = 5;
	private int numElemProcessed, numPop = 0;
	private static final Log LOG = LogFactory.getLog(HWorldPseudoReducer.class.getName());
	//Cada posicion del array del torneo sera un Hashtable, ya que necesitamos almacenar al individuo y su fitness..
	private Hashtable[]tournArray = new Hashtable [2*tournamentSize]; 
	private Text[]crossArray = new Text [crossSize];
	private Hashtable parameters = new Hashtable();
	//Indicamos el fichero de configuracion que debera leer
	final String HDFS_REDUCER_CONFIGURATION_FILE="/user/hadoop-user/data/reducer_configuration.dat";
	private Random r = new Random(System.nanoTime());

	
	@Override
	protected void setup(Context cont) throws IOException{
		LOG.info("***********DENTRO DEL SETUP DEL REDUCER**********");
		FileSystem hdfs = FileSystem.get(new Configuration()); 
		Path path = new Path(HDFS_REDUCER_CONFIGURATION_FILE);
		
		
		//Validamos primero el path de entrada antes de leer del fichero
		if (!hdfs.exists(path))
		{
			LOG.info("***********RSETUP:NO EXISTE EL FICHERO**********");
			throw new IOException("El fichero especificado " +HDFS_REDUCER_CONFIGURATION_FILE + "no existe");
			
		}
		
		if (!hdfs.isFile(path))
		{
			LOG.info("***********RSETUP: NO ES UN FICHERO VALIDO**********");
			throw new IOException("El fichero especificado "+HDFS_REDUCER_CONFIGURATION_FILE + "no es un fichero valido");
		}
		
		FSDataInputStream dis = hdfs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(dis));
		String strLine;
		String[]keys = {"numPopulation","maxIterations","elitRate","mutationRate","mutation","targetPhrase"};
		int index=0;
		 while ((strLine = br.readLine()) != null)   {
			parameters.put(keys[index], strLine);
	        index++;
	      }
		 dis.close();
		 numPop = Integer.parseInt((String)parameters.get("numPopulation"));
	}
	
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		Iterator<IntWritable> valuesIter =values.iterator();

		/**TODO:Si esta presente el elitismo, lo vamos a escribir directamente en el contexto
		 * con un fitness cualquiera
		 */
		
		while (valuesIter.hasNext()) {
			fitness = valuesIter.next();
			LOG.info("EL NUMERO DE ELEMENTOS PROCESADOS ES " +numElemProcessed);
			//LOG.info("LA CLAVE DEL DESCENDIENTE ACTUAL ES "+key+" Y SU FITNESS ES "+fitness);	
			int currentPos = (numElemProcessed % tournamentSize);
			tournArray[currentPos] = new Hashtable();
			tournArray[currentPos].put(key.toString(), fitness.get());
			numElemProcessed++;	
			//Cuando tengamos [tournamentSize] elementos celebramos el torneo...
			if (numElemProcessed % tournamentSize == 0){
				LOG.info("*****CELEBRAMOS EL TORNEO******");
				selectionAndCrossover(numElemProcessed, fitness, context,tournArray);
			}
		}
		//Si todos los elementos han sido procesados...
		if(numElemProcessed == numPop - 1) {
			closeAndWrite(fitness, context);
		}	
	}
	
	public void closeAndWrite(IntWritable fitness, Context context) {
		LOG.info("*****TODOS LOS ELEMENTOS HAN SIDO PROCESADOS******");
		// Cleanup for the last window of tournament
		for(int k=0; k<tournamentSize; k++) {
			// Conduct a tournament over the past window				
			selectionAndCrossover(numElemProcessed, fitness, context,tournArray);
			numElemProcessed++;
		}
	}
	
	private void selectionAndCrossover(int numElemProcessed, IntWritable fitness, Context context, Hashtable[]tournArray){
		String tournWinner = this.tournSelection(tournArray);
		Text textWinner = new Text(tournWinner);
		crossArray[numElemProcessed % crossSize] = textWinner; 
		//LOG.info("DENTRO DE SELECTIONANDCROSSOVER LA POSICION EN LA QUE INSERTO ES " +numElemProcessed % crossSize);
		LOG.info("DENTRO DE SELECTIONANDCROSSOVER EL GANADOR DEL TORNEO ES " +tournWinner);
		if (((numElemProcessed - tournamentSize) % crossSize) == (crossSize - 1)) 
		{
			// Do crossover every odd iteration between successive individuals...
			Text[] newIndividuals = crossOver(crossArray);
			try {
				  for(int i=0;i < newIndividuals.length;i++)
				  {
					LOG.info("******ESCRITURA EN EL CROSSOVER*****");
					LOG.info("DENTRO DE SELECTIONANDCROSSOVER EL VALOR["+i+"]QUE ESCRIBO ES " +newIndividuals[i]);  
				    context.write(newIndividuals[i], fitness);
				  }
				} catch(ArrayIndexOutOfBoundsException aioobe) {} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}	
	}
	
	/**Implementamos el metodo de seleccion por torneo sin reemplazamiento*/
	private String tournSelection(Hashtable[]tournArray) {
		/**Dentro del array de contendientes, elegimos al que tenga mejor fitness para
		 * luego cruzarlo... 
		 */
		String tournWinner = new String();
		String gladKey = new String();
		int bestFitness = 999999;
		int gladFitness = 0;
		
		for (int i=0;i<=(tournamentSize-1);i++) {
			Hashtable gladiator = tournArray[i];
			LOG.info("DENTRO DE TOURNSELECTION EL TOURNARRAY["+i+"] VALE: "+tournArray[i]);
			Enumeration<Integer> e = gladiator.elements();
			Enumeration<String> keys = gladiator.keys();
			gladFitness = e.nextElement();
			gladKey = (String)keys.nextElement();
			 
		if (gladFitness < bestFitness) {
			bestFitness = gladFitness;
			tournWinner = gladKey;
			}
		}	
		
		return tournWinner;
	}

	
	
	
	//Operacion de cruce sobre dos individuos...
	private Text[] crossOver(Text[]crossArray) {
		//LOG.info("EN EL CROSSOVER,LA LONGITUD DEL CROSSARRAY ES "+crossArray.length);
		//Declaramos el array de texto de los nuevos individuos tras el cruce...
		Text[] newIndividuals = new Text[crossArray.length];
		
		String parent1 = crossArray[0].toString();
		String parent2 = crossArray[1].toString();
		
		//Establecemos el punto de corte para ver como se generan los descendientes
		int cutPoint = (int) ((Math.random()*(parent1.length()- 1))+ 1);
		//LOG.info("EL PUNTO DE CORTE EN EL CROSSOVER ES: "+cutPoint);
		
		//Creamos las partes identicas a las de los padres...
		String child1part1 = parent1.substring(0, cutPoint -1);
		String child2part1 = parent2.substring(0, cutPoint -1);
		
		//Cruzamos el resto del descendiente...
		String child1part2 = parent2.substring(cutPoint,parent2.length());
		String child2part2 = parent1.substring(cutPoint,parent1.length());
		
		//Concatenamos las partes...
		String child1 = child1part1.concat(child1part2);
		String child2 = child2part1.concat(child2part2);
		
		
		Text descend1 = new Text(child1);
		Text descend2 = new Text(child2);
		
		//Sustituimos los valores de los padres por el de los descendientes...
		//crossArray[r1] = descend1;
		//crossArray[r2] = descend2;
		
		//Generamos un nuevo array con los descendientes..
		newIndividuals[0] = descend1;
		newIndividuals[1] = descend2;
		
		return newIndividuals;
		
	}
	
	
	/**Mutamos al individuo concreto**/
	private void mutate(Text individual)
	{
		double random = r.nextDouble();
		double mutationRate = (Double)parameters.get("mutationRate");
		
		//Si el numero aleatorio cae dentro del rango de mutacion, seguimos...
		if (random < mutationRate) {
			String sText = individual.toString();
			int popLength = individual.getLength();
			char[] arr = sText.toCharArray();  
			
			//Obtenemos dos posiciones aleatorias dentro del Individuo...
			int r1 = r.nextInt(popLength-1);
			int r2 = r.nextInt(popLength-1);
			
			//Obtenemos los genes que se encuentran en esas posiciones...
			char g1 = sText.charAt(r1);
			char g2 = sText.charAt(r2);
			
			//Intercambiamos las posiciones de esos genes...
			arr[r1] = g2;
			arr[r2] = g1;
			individual.set(arr.toString());
		}
	}
}
