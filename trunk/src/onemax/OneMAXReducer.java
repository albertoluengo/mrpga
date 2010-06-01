package onemax;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import targetphrase.TargetPhraseReducer;


public class OneMAXReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	private DoubleWritable fitness = new DoubleWritable();
	private int crossSize = 2;
	private int tournamentSize = 5;
	private int numElemProcessed, numPop, boolElit, mutation, cont, numTournaments = 0;
	private static final Log LOG = LogFactory.getLog(OneMAXReducer.class.getName());
	
	private String[][]tournamentArray;
	private String[]tournIndiv;
	private double[]tournamentFitness = new double[tournamentSize];
	private double[]tournamentGroupFitness = new double[2*tournamentSize];
	
	//Cada posicion del array del torneo sera un Hashtable, ya que necesitamos almacenar al individuo y su fitness..
	//private Hashtable[]tournArray = new Hashtable [2*tournamentSize];
	private Hashtable[]tournArray = new Hashtable [tournamentSize];
	private String[][]crossArray = new String [tournamentSize][tournamentSize];
	private Hashtable parameters = new Hashtable();
	private Hashtable bestIndivTable = new Hashtable();
	private String USERNAME = "";
	private Random r;
	private Text bestInd = new Text("");
	private double bestIndFitness = 0.0;
	private double mutationRate, crossProb = 0.0;
	private Vector bufferWinners = new Vector();

	OneMAXReducer() {
		r = new Random(System.nanoTime());
	}
	
	@Override
	protected void setup(Context cont) throws IOException{
		//LOG.info("***********DENTRO DEL SETUP DEL REDUCER**********");
		FileSystem hdfs = FileSystem.get(new Configuration()); 
		Configuration conf = cont.getConfiguration();
		String users = conf.get("hadoop.job.ugi");
		String[] commas = users.split(",");
		USERNAME = commas[0];
		String HDFS_REDUCER_CONFIGURATION_FILE="/user/"+USERNAME+"/data/reducer_configuration.dat";
		String BEST_INDIVIDUAL_FILE="/user/"+USERNAME+"/bestIndividuals/bestIndiv.txt";
		Path path = new Path(HDFS_REDUCER_CONFIGURATION_FILE);
		Path bestIndPath = new Path(BEST_INDIVIDUAL_FILE);
		
		
		//Validamos primero los path de entrada antes de leer del fichero
		if (!hdfs.exists(path))
		{
			LOG.info("***********RSETUP:NO EXISTE EL FICHERO**********");
			throw new IOException("ALGUNO DE LOS FICHEROS DE CONFIGURACION NO EXISTE");	
		}
		
		if (!hdfs.isFile(path))
		{
			LOG.info("***********RSETUP: NO ES UN FICHERO VALIDO**********");
			throw new IOException("ALGUNO DE LOS FICHEROS ESPECIFICADOS NO ES VALIDO");
		}
		
		
		FSDataInputStream dis = hdfs.open(path);
		FSDataInputStream dis2 = hdfs.open(bestIndPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(dis));
		BufferedReader br2 = new BufferedReader(new InputStreamReader(dis2));
		String strLine;
		String[]keys = {"numPopulation","maxIterations","boolElit","mutationRate","mutation","crossProb","targetPhrase"};
		String[]bestIndKeys = {"bestIndiv","bestFitness"};
		int index = 0;
		int index2=0;
		while ((strLine = br.readLine()) != null)   {
			parameters.put(keys[index], strLine);
		    index++;
		  }
		while ((strLine = br2.readLine()) != null)   {
			bestIndivTable.put(bestIndKeys[index2], strLine);
		    index2++;
		  }
		
		dis.close();
		numPop = Integer.parseInt((String)parameters.get("numPopulation"));
		boolElit = Integer.parseInt((String)parameters.get("boolElit"));
		mutation = Integer.parseInt((String)parameters.get("mutation"));
		mutationRate = Double.parseDouble((String)parameters.get("mutationRate"));
		crossProb = Double.parseDouble((String)parameters.get("crossProb"));
		
		//tournIndiv = new String[targetPhrase.length()];
		tournIndiv = new String[tournamentSize];
		//tournamentArray = new String[2*tournamentSize][targetPhrase.length()];
		tournamentArray = new String[tournamentSize][tournamentSize];
		
		
		/**Si esta activada la opcion del elitismo, 
		 * escribimos el mejor elemento en la salida...
		 */
		if (boolElit == 1) {
			bestInd = new Text((String)bestIndivTable.get("bestIndiv"));
			bestIndFitness = Double.parseDouble((String)bestIndivTable.get("bestFitness"));
			try {
				cont.write(bestInd, new DoubleWritable(bestIndFitness));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
	{
		
		Iterator<DoubleWritable> valuesIter =values.iterator();
		
		while (valuesIter.hasNext()) 
		{
			fitness = valuesIter.next();
			//LOG.info("EL INDIVIDUO "+numElemProcessed+" TIENE CLAVE "+key.toString());
			
//			tournIndiv[numElemProcessed%tournamentSize + tournamentSize] = key.toString();
//			tournamentFitness[numElemProcessed%tournamentSize + tournamentSize] = fitness.get();
			
			tournIndiv[numElemProcessed%tournamentSize] = key.toString();
			tournamentFitness[numElemProcessed%tournamentSize] = fitness.get();
			
			//Cada <tournamentSize> iteraciones, meto los arrays en las posiciones del array de arrays...
			//Esperamos que se unan los participantes del torneo...
			if ((numElemProcessed % (tournamentSize))==0 && numElemProcessed!=0 && cont!=tournamentSize &&numTournaments==0)
			{
				//Calculamos el mejor fitness de los elementos del grupo...
				LOG.info("*****CALCULAMOS LOS VALORES GRUPALES******");
				double bestGroupFitness =  -99999;
				for (int i=0; i < tournamentFitness.length;i++)
				{
					if (tournamentFitness[i] > bestGroupFitness)
						bestGroupFitness = tournamentFitness[i];
					//LOG.info("TOURNFIT["+i+"] VALE "+tournamentFitness[i]);
					//LOG.info("BESTGROUPFITNESS VALE "+bestGroupFitness);
				}	
				//int currentPos = (numElemProcessed%tournamentSize) + cont;
				//tournamentArray[cont] = tournIndiv;
				//OJO!PROBLEMA EN LA ASIGNACION DIRECTA DE ARRIBA!
				for (int i = 0; i < tournIndiv.length; i++) {
					tournamentArray[cont][i] = tournIndiv[i]; 
				}
				
				for (int a = 0; a<tournArray.length;a++) {
					String[]stringArr = tournamentArray[a];
					for (int b = 0; b<stringArr.length;b++) {
						//LOG.info("LOS ELEMENTOS DEL TOURNARRAY "+b+" VALE "+stringArr[b]);
					}
				}
				//tournamentGroupFitness[numElemProcessed%tournamentSize + cont] = bestGroupFitness;
				tournamentGroupFitness[cont] = bestGroupFitness;
				
				//LOG.info("TOURNAMENTGROUPFITNESS["+cont+"] METO "+tournamentGroupFitness[cont]);
				cont++;
				//LOG.info("CONT VALE "+cont);
			}
			//Cuando tengamos [tournamentSize] elementos en el array de arrays celebramos el torneo...
			if ((cont ==tournamentSize) && (numTournaments == 0))
			{
				//LOG.info("*****CELEBRO EL PRIMER TORNEO******");
//				for (int a = 0; a<tournArray.length;a++) {
//					String[]stringArr = tournamentArray[a];
//					for (int b = 0; b<stringArr.length;b++) {
//						//LOG.info("LOS ELEMENTOS DEL TOURNARRAY "+b+" VALE "+stringArr[b]);
//					}
//				}
//				for (int b = 0; b<tournamentGroupFitness.length;b++) {
//					//LOG.info("LOS ELEMENTOS DE TOURNAMENTGROUPFITNESS "+b+" VALE "+tournamentGroupFitness[b]);
//					}
				selectionAndCrossover(numElemProcessed, tournamentArray, context);
				numTournaments++;
				numElemProcessed++;
				continue;
			}
			
			if (numTournaments!=0) 
			{
				//LOG.info("*****CELEBRO EL RESTO DE TORNEOS******");
				//Si no es el primer torneo que celebramos, solo esperamos
				//por el siguiente participante (5 elementos más) y sobreescribimos...
				double bestGroupFitness = -99999;
				for (int i = 0; i < tournIndiv.length; i++) {
					tournamentArray[numTournaments%(tournamentSize -1)][i] = tournIndiv[i];
					if (tournamentFitness[i] > bestGroupFitness)
						bestGroupFitness = tournamentFitness[i];
				}
				tournamentGroupFitness[numTournaments%(tournamentSize -1)] = bestGroupFitness;
						
				selectionAndCrossover(numElemProcessed, tournamentArray, context);
				numTournaments++;
			}
			numElemProcessed++;
		}
		//Si todos los elementos han sido procesados...
		if(numElemProcessed == numPop) {
			closeAndWrite(context);
		}	
	}
	
	public void closeAndWrite(Context context) throws IOException, InterruptedException {
		LOG.info("*****TODOS LOS ELEMENTOS HAN SIDO PROCESADOS******");
		
		double bestGroupFitness = -99999;
		for (int i = 0; i < tournIndiv.length; i++) {
			tournamentArray[numTournaments%(tournamentSize -1)][i] = tournIndiv[i];
			if (tournamentFitness[i] > bestGroupFitness)
				bestGroupFitness = tournamentFitness[i];
		}
		tournamentGroupFitness[numTournaments%(tournamentSize -1)] = bestGroupFitness;
		selectionAndCrossover(numElemProcessed, tournamentArray, context);
	}
	
	private void selectionAndCrossover(int numElemProcessed, String[][]tournArray,Context context){
		String[] tournWinner = this.tournSelection(tournArray);
		String[][] newIndividuals = null;
		crossArray[numElemProcessed % crossSize] = tournWinner; 
		//LOG.info("DENTRO DE SELECTIONANDCROSSOVER EL GANADOR DEL TORNEO ES " +tournWinner);
		//LOG.info("EL TAMANHO DE LA POBLACION ES "+numPop);
		if (((numElemProcessed - tournamentSize) % crossSize) == (crossSize - 1) && (bufferWinners.size() <= numPop)) 
		{
			if (crossProb < r.nextDouble()) 
				newIndividuals = crossOver();
			else
				newIndividuals = crossArray;
			try 
			{
			  for(int i=0;i < crossSize;i++)
			  {	  
				//LOG.info("******ESCRITURA EN SELECTIONANDCROSSOVER*****");
				String[] individuals = newIndividuals[i];
				for (int j=0;j<individuals.length;j++)
				{
					/**
					 * Tengo que mirar si el individuo ya existe en el "buffer";
					 * es decir, si salio como ganador de un torneo una vez y
					 * vuelve a salir, no se escribe...
					 */
					if (bufferWinners.contains(individuals[j]))
						continue;
					//Escribimos en el fichero y en el buffer de ganadores...
					LOG.info("DENTRO DE SELECTIONANDCROSSOVER EL VALOR["+j+"]QUE ESCRIBO ES " +individuals[j]);
					Text indiv = new Text(individuals[j]);
					/**Para no caer en mesetas o maximos locales, vamos a mutar a los
					 * individuos antes de cruzarlos...
					 */
					if (mutation == 1)
						context.write(this.mutate(indiv), fitness);
					else
						context.write(indiv, fitness);
					//Escribo en el buffer de ganadores...
					bufferWinners.addElement(individuals[j]);
				}
			  }
			}
			catch(ArrayIndexOutOfBoundsException aioobe) {
				aioobe.printStackTrace();
			} 
			catch (IOException e) {
				e.printStackTrace();
			} 
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			catch (NullPointerException e) {
			}
			
		}	
	}
	
	/**Implementamos el metodo de seleccion por torneo sin reemplazamiento*/
	private String[] tournSelection(String[][]tournArray) {
		/**Dentro del array de arrays de contendientes, elegimos al que tenga mejor fitness para
		 * luego cruzarlo... 
		 */
		String[] tournWinner = null;
		double bestFitness = -999999;
		
		for (int i=0;i <tournamentSize ;i++) {
			//LOG.info("TOURNAMENTGROUPFITNESS["+i+"] VALE "+tournamentGroupFitness[i]);
			if (tournamentGroupFitness[i] > bestFitness)
			{
				bestFitness = tournamentGroupFitness[i];
				//LOG.info("DENTRO DE TOURNSELECTION, LOS FITNESS VALEN "+bestFitness);
				tournWinner = tournArray[i];
			}
		}
		//LOG.info("EL MEJOR FITNESS DENTRO DEL TOURNSELECTION ES "+bestFitness);
		for (int aux = 0; aux<tournWinner.length;aux++) {
			//LOG.info("TOURNWINNER "+aux+" VALE "+tournWinner[aux]);
		}
		
		return tournWinner;
	}

	
	
	//Operacion de cruce sobre dos grupos de individuos...
	private String[][] crossOver() {
		LOG.info("*********EN EL CROSSOVER**********");
		String[][] newIndividuals = new String[crossArray.length][tournamentSize];
		
		String[] parent1 = crossArray[0];
		String[] parent2 = crossArray[1];
		
		
//		LOG.info("PARENT1 LEN ES: "+parent1.length);
//		LOG.info("PARENT2 LEN ES: "+parent2.length);
		
		//Establecemos el punto de corte para ver como se generan los descendientes
		//int cutPoint = (int) ((Math.random()*(parent1.length- 1))+ 1);
		int cutPoint = (int) ((Math.random()*(parent1[0].length()- 1))+ 1);
		//LOG.info("EL PUNTO DE CORTE EN EL CROSSOVER ES: "+cutPoint);
		
		
		//Creamos las partes identicas a las de los padres...
		String[] child1 = new String[parent1.length];
		String[] child2 = new String[parent2.length];
		
		for (int aux = 0; aux<parent1.length;aux++) {
			//LOG.info("PARENT1["+aux+"] VALE "+parent1[aux]);
			//LOG.info("PARENT2["+aux+"] VALE "+parent2[aux]);
			String p1 = parent1[aux];
			String p2 = parent2[aux];
			String child1P1 = p1.substring(0, cutPoint);
			String child1P2 = p2.substring(cutPoint, (parent1[aux].length()));
			String child2P1 = p2.substring(0, cutPoint);
			String child2P2 = p1.substring(cutPoint, (parent1[aux].length()));
			
			//Concatenamos...
			child1[aux] = child1P1+child1P2;
			//LOG.info("CHILD1["+aux+"] VALE "+child1[aux]);
			child2[aux] = child2P1+child2P2;
			//LOG.info("CHILD2["+aux+"] VALE "+child2[aux]);
		}
				
		//Creamos un nuevo array de arrays...
		newIndividuals[0] = child1;
		newIndividuals[1] = child2;
		
		return newIndividuals;
	}
	
	
	/**Mutamos al individuo concreto**/
	private Text mutate(Text individual)
	{
		double random = r.nextDouble();
		String sText = individual.toString();
		String mutInd = "";
		int beginIndex = 0, endIndex = 0;
		
		//Si el numero aleatorio cae dentro del rango de mutacion, seguimos...
		if (random < mutationRate) {
			//LOG.info("**MUTAMOS AL INDIVIDUO "+individual+" *****");
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
				//LOG.info("****** EL INDIVIDUO MUTADO ES "+mutInd+" *****");
			}
		}
		//...si no, devolvemos el individuo tal cual...
		else {
			mutInd = sText;
		}
		return new Text(mutInd);
	}
}
