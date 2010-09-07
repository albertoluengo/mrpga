package common;

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


/**
 * Clase que representa un nodo <code>Reducer</code> dentro del framework 
 * de ejecuci&#243;n <code>MapReduce</code>, implementando todas las funciones
 * necesarias para ello (<code>reduce()</code>, <code>setup()</code>, etc).
 * @author Alberto Luengo Cabanillas
 */
public abstract class MRPGAReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

	private DoubleWritable fitness = new DoubleWritable();
	private int crossSize = 2;
	private int tournamentSize = 0;
	private int numElemProcessed, numPop, boolElit, mutation, cont, numTournaments = 0;
	private static final Log LOG = LogFactory.getLog(MRPGAReducer.class.getName());
	
	private String[][]tournamentArray;
	private String[]tournIndiv;
	private double[]tournamentFitness;
	private double[]tournamentGroupFitness;
	//Cada posicion del array del torneo sera un Hashtable, ya que necesitamos almacenar al individuo y su fitness..
	private Hashtable[]tournArray;
	private String[][]crossArray;
	private Hashtable reduceParameters = new Hashtable();
	private Hashtable problemConfigParameters = new Hashtable();
	private Hashtable bestIndivTable = new Hashtable();
	private String USERNAME = "";
	private Random r;
	private Text bestInd = new Text("");
	private int bestIndFitness = 0;
	private double crossProb = 0.0;
	private String critFitness ="";
	private Vector bufferWinners = new Vector();
	private int indWinner = 0;

	/**
	 * M&#233;todo constructor de la clase <code>TargetPhraseReducer</code>
	 * que inicializa una nueva semilla para la generaci&#243;n de n&#250;meros aleatorios.
	 */
	public MRPGAReducer() {
		r = new Random(System.nanoTime());
	}
	
	
	public abstract void problemSetup(Hashtable configParams, Hashtable mappersParams);
	/**
	 * M&#233;todo <code>override</code> que se ejecutar&#225; una &#250;nica vez en el sistema
	 * que servir&#225; para leer y parsear los par&#225;metros de configuraci&#243;n necesarios
	 * para los nodos <code>Reducer</code>.
	 * @param cont Contexto en el que se ejecuta el trabajo <code>MapReduce</code>.
	 * @throws IOException Excepci&#243;n que se lanza si ha habido alg&#250;n error manipulando ficheros o directorios.
	 */
	@Override
	protected void setup(Context cont) throws IOException{
		FileSystem hdfs = FileSystem.get(new Configuration()); 
		Configuration conf = cont.getConfiguration();
		String users = conf.get("hadoop.job.ugi");
		String[] commas = users.split(",");
		USERNAME = commas[0];
		String HDFS_GENERAL_PARAMS_FILE="/user/"+USERNAME+"/data/general_params.dat";
		String HDFS_PROBLEM_PARAMS_FILE="/user/"+USERNAME+"/data/problem_params.dat";
		Path path = new Path(HDFS_GENERAL_PARAMS_FILE);
		Path problemParamsPath = new Path(HDFS_PROBLEM_PARAMS_FILE);
		
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
		BufferedReader br = new BufferedReader(new InputStreamReader(dis));
		String strLine;
		String[]keys = {"numReducers", "numIterations","numPopulation", 
				"popPerMapper","gene_length","chromKind","boolElit", 
				"mutation", "mutationRate", "crossProb", "tournWindow", "debugging", "endCriterial",
				"userDir","userName"};
		int index = 0;
		while ((strLine = br.readLine()) != null)   {
			String lineValue = strLine.substring(strLine.indexOf(':')+1,strLine.length());
			reduceParameters.put(keys[index], lineValue);
		    index++;
		  }
		dis.close();
		
		FSDataInputStream dis2 = hdfs.open(problemParamsPath);
		BufferedReader br2 = new BufferedReader(new InputStreamReader(dis2));
		int numLine=0;
		 while ((strLine = br2.readLine()) != null)   {
			String lineKey = strLine.substring(0,strLine.indexOf(':'));
			String lineValue = strLine.substring(strLine.indexOf(':')+1,strLine.length());
			problemConfigParameters.put(lineKey, lineValue);
	        numLine++;
	      }
		 dis2.close();
		
		
		numPop = Integer.parseInt((String)reduceParameters.get("numPopulation"));
		boolElit = Integer.parseInt((String)reduceParameters.get("boolElit"));
		mutation = Integer.parseInt((String)reduceParameters.get("mutation"));
		crossProb = Double.parseDouble((String)reduceParameters.get("crossProb"));
		tournamentSize = Integer.parseInt((String)reduceParameters.get("tournWindow"));
		critFitness = (String)problemConfigParameters.get("bestFitness");
		
		//Inicializamos las estructuras auxiliares una vez hemos leido 'tournamentSize'...
		tournIndiv = new String[tournamentSize];
		tournamentArray = new String[tournamentSize][tournamentSize];
		tournamentFitness = new double[tournamentSize];
		tournamentGroupFitness = new double[2*tournamentSize];
		tournArray = new Hashtable [tournamentSize];
		crossArray = new String [tournamentSize][tournamentSize];
		
		/**Si esta activada la opcion del elitismo, 
		 * escribimos el mejor elemento en la salida...
		 */
		if (boolElit == 1) {
			String BEST_INDIVIDUAL_FILE="/user/"+USERNAME+"/bestIndividuals/bestIndiv.dat";
			Path bestIndPath = new Path(BEST_INDIVIDUAL_FILE);
			
			int index2=0;
			FSDataInputStream dis3 = hdfs.open(bestIndPath);
			BufferedReader br3 = new BufferedReader(new InputStreamReader(dis3));
			String[]bestIndKeys = {"bestIndiv","bestFitness"};
			while ((strLine = br3.readLine()) != null)   {
				bestIndivTable.put(bestIndKeys[index2], strLine);
			    index2++;
			  }
			dis3.close();

			bestInd = new Text((String)bestIndivTable.get("bestIndiv"));
			bestIndFitness = (int)Double.parseDouble((String)bestIndivTable.get("bestFitness"));
			try {
				cont.write(bestInd, new DoubleWritable(bestIndFitness));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}	
		}
		//Miramos si es necesario realizar alguna inicialización específica del problema
		problemSetup(problemConfigParameters,reduceParameters);
	}
	
	/**
	 * MM&#233;todo <code>override</code> que recibe los distintos pares (clave,valor) 
	 * de alg&#250;n nodo <code>Mapper</code> y los procesa de acuerdo a una serie de reglas, devolviendo una lista de
	 * pares (clave,valor) al contexto.
	 * @param key La clave del par (clave,valor) que genera este m&#233;todo.
	 * @param values El conjunto de todos los valores fitness de los individuos..
	 * @param context Contexto en el que se ejecuta el trabajo <code>MapReduce</code>.
	 * @throws IOException Excepci&#243;n que se lanza si ha habido alg&#250;n error manipulando ficheros o directorios.
	 * @throws InterruptedException Excepción propia de <code>Hadoop</code> que se lanza si se interrumpe alguna transacci&#243;n at&#243;mica.
	 */
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
	{
		Iterator<DoubleWritable> valuesIter =values.iterator();
		while (valuesIter.hasNext()) 
		{
			fitness = valuesIter.next();
			tournIndiv[numElemProcessed%tournamentSize] = key.toString();
			tournamentFitness[numElemProcessed%tournamentSize] = fitness.get();
			
			//Cada <tournamentSize> iteraciones, meto los arrays en las posiciones del array de arrays...
			//Esperamos que se unan los participantes del torneo...
			if ((numElemProcessed % (tournamentSize))==0 && numElemProcessed!=0 && cont!=tournamentSize &&numTournaments==0)
			{
				//Calculamos el mejor fitness de los elementos del grupo...
				//LOG.info("*****CALCULAMOS LOS VALORES GRUPALES******");
				double bestGroupFitness = 0.0;
				if (critFitness.equals("minor"))
				{
					bestGroupFitness = 99999.0;
					for (int i=0; i < tournamentFitness.length;i++)
					{
						if (tournamentFitness[i] < bestGroupFitness)
							bestGroupFitness = tournamentFitness[i];
						//LOG.info("TOURNFIT["+i+"] VALE "+tournamentFitness[i]);
						//LOG.info("BESTGROUPFITNESS VALE "+bestGroupFitness);
					}
				}
				else
				{
					bestGroupFitness = -99999.0;
					for (int i=0; i < tournamentFitness.length;i++)
					{
						if (tournamentFitness[i] > bestGroupFitness)
							bestGroupFitness = tournamentFitness[i];
						//LOG.info("TOURNFIT["+i+"] VALE "+tournamentFitness[i]);
						//LOG.info("BESTGROUPFITNESS VALE "+bestGroupFitness);
					}
				
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
				double bestGroupFitness = 0.0;
				if (critFitness.equals("minor"))
				{
					bestGroupFitness = 99999.0;
					for (int i = 0; i < tournIndiv.length; i++) {
						tournamentArray[indWinner][i] = tournIndiv[i];
						if (tournamentFitness[i] < bestGroupFitness)
							bestGroupFitness = tournamentFitness[i];
					}
				}
				else 
				{
					bestGroupFitness = -99999.0;
					for (int i = 0; i < tournIndiv.length; i++) {
						tournamentArray[indWinner][i] = tournIndiv[i];
						if (tournamentFitness[i] > bestGroupFitness)
							bestGroupFitness = tournamentFitness[i];
					}
				}
				tournamentGroupFitness[indWinner] = bestGroupFitness;
						
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
	
	/**
	 * M&#233;todo que ejecuta el &#250;ltimo torneo de individuos cuando todos los usuarios
	 * han sido procesados. Esto se debe a la ventana activa de <code>tournamentSize</code>
	 * elementos con los que se trabaja
	 * @param context Instancia de la clase <code>Context</code> que facilita informaci&#243;n acerca
	 * del trabajo <code>MapReduce</code> que se est&#225; ejecutando. 
	 * @throws IOException Excepci&#243;n lanzada al haber alg&#250;n problema manipulando
	 * ficheros o directorios.
	 * @throws InterruptedException Excepci&#243;n propia del API de Hadoop que se lanza si hay alg&#250;	n
	 * problema escribiendo los individuos procesados.
	 * @throws IOException Excepci&#243;n que se lanza si ha habido alg&#250;n error manipulando ficheros o directorios.
	 * @throws InterruptedException Excepción propia de <code>Hadoop</code> que se lanza si se interrumpe alguna transacci&#243;n at&#243;mica.
	 */
	public void closeAndWrite(Context context) throws IOException, InterruptedException {
		//LOG.info("*****TODOS LOS ELEMENTOS HAN SIDO PROCESADOS******");
		//Acabamos con la ultima ventana del torneo...
		for (int lastIter=0; lastIter <tournamentSize;lastIter++)
		{
			double bestGroupFitness = 0.0;
			if (critFitness.equals("minor"))
			{
				bestGroupFitness = 99999.0;
				for (int i = 0; i < tournIndiv.length; i++) {
					tournamentArray[indWinner][i] = tournIndiv[i];
					if (tournamentFitness[i] < bestGroupFitness)
						bestGroupFitness = tournamentFitness[i];
				}
			}
			else 
			{
				bestGroupFitness = -99999.0;
				for (int i = 0; i < tournIndiv.length; i++) {
					tournamentArray[indWinner][i] = tournIndiv[i];
					if (tournamentFitness[i] > bestGroupFitness)
						bestGroupFitness = tournamentFitness[i];
				}
			}
			tournamentGroupFitness[indWinner] = bestGroupFitness;
			selectionAndCrossover(numElemProcessed, tournamentArray, context);
			numElemProcessed += lastIter;
		}
		
	}
	
	/**
	 * M&#233;todo que se encarga de escribir los distintos resultados sub-&#243;ptimos en
	 * el fichero de salida correspondiente, tras haber realizado una selecci&#243;n
	 * de los mejores individuos por medio de un torneo, cruzarlos y, si se ha
	 * indicado, mutarlos.
	 * @param numElemProcessed N&#250;mero de individuos procesados de una poblaci&#243;n.
	 * @param tournArray Conjunto de individuos que participar&#225;	n en el torneo.
	 * @param context Instancia de la clase <code>Context</code> que facilita informaci&#243;n acerca
	 * del trabajo <code>MapReduce</code> que se est&#225;	 ejecutando.
	 */
	private void selectionAndCrossover(int numElemProcessed, String[][]tournArray,Context context){
		String[] tournWinner = this.tournSelection(tournArray);
		String[][] newIndividuals = null;
		crossArray[numElemProcessed % crossSize] = tournWinner; 
		//LOG.info("DENTRO DE SELECTIONANDCROSSOVER EL GANADOR DEL TORNEO ES " +tournWinner);
		if (((numElemProcessed - tournamentSize) % crossSize) == (crossSize - 1) && (bufferWinners.size() <= numPop)) 
		{
			if (r.nextDouble() > crossProb)
				try {
					newIndividuals = crossOver(reduceParameters, problemConfigParameters, crossArray);
				}catch (Exception e) {
					e.printStackTrace();
					System.err.println("MRPGAREDUCER: Se ha producido un error en la UDF 'crossOver'\n. No se cruzan los individuos...");
					newIndividuals = crossArray;
				}
			else
				newIndividuals = crossArray;
			try 
			{
			  for(int i=0;i < crossSize;i++)
			  {	  
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
					//LOG.info("DENTRO DE SELECTIONANDCROSSOVER EL VALOR["+j+"]QUE ESCRIBO ES " +individuals[j]);
					Text indiv = new Text(individuals[j]);
					/**Para no caer en mesetas o maximos locales, vamos a mutar a los
					 * individuos antes de cruzarlos...
					 */
					if (mutation==1)
						try {
							indiv = new Text(mutate(reduceParameters, problemConfigParameters, individuals[j]));
						}catch (Exception e) {
							e.printStackTrace();
							System.err.println("MRPGAREDUCER: Se ha producido un error en la UDF 'mutate'\n. No se muta el individuo...");
						}
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
		}	
	}
	
	/**
	 * M&#233;todo que implementa el metodo de seleccion por torneo sin reemplazamiento
	 * entre los distintos individuos de una poblaci&#243;n, devolviendo los ganadores
	 * del mismo.
	 * @param tournArray Conjunto de individuos (contendientes) sobre los que realizar el torneo.
	 * @return Conjunto de individuos ganadores del torneo.
	 */
	private String[] tournSelection(String[][]tournArray) {
		/**Dentro del array de arrays de contendientes, elegimos al que tenga mejor fitness para
		 * luego cruzarlo... 
		 */
		String[] tournWinner = null;
		double bestFitness = 0.0;
		Vector indWinners = new Vector();
		
		if (critFitness.equals("minor"))
		{
			bestFitness = 99999.0;
			for (int i=0;i <tournamentSize ;i++) {
				//LOG.info("TOURNAMENTGROUPFITNESS["+i+"] VALE "+tournamentGroupFitness[i]);
				if (tournamentGroupFitness[i] < bestFitness)
				{
					bestFitness = tournamentGroupFitness[i];
					//LOG.info("DENTRO DE TOURNSELECTION, LOS FITNESS VALEN "+bestFitness);
					tournWinner = tournArray[i];
					indWinners.addElement(i);
				}
			}
		}
		else 
		{
			bestFitness=-99999.0;
			for (int i=0;i <tournamentSize ;i++) {
				//LOG.info("TOURNAMENTGROUPFITNESS["+i+"] VALE "+tournamentGroupFitness[i]);
				if (tournamentGroupFitness[i] > bestFitness)
				{
					bestFitness = tournamentGroupFitness[i];
					//LOG.info("DENTRO DE TOURNSELECTION, LOS FITNESS VALEN "+bestFitness);
					tournWinner = tournArray[i];
					indWinners.addElement(i);
				}
			}
		}
		//LOG.info("EL MEJOR FITNESS DENTRO DEL TOURNSELECTION ES "+bestFitness);
//		for (int aux = 0; aux<tournWinner.length;aux++) {
//			//LOG.info("TOURNWINNER "+aux+" VALE "+tournWinner[aux]);
//		}
		//Sacamos el indice del elemento ganador para escribir el siguiente
		//elemento en él...
		indWinner = Integer.parseInt(indWinners.elementAt((indWinners.size()-1)).toString());
		
		return tournWinner;
	}
	/**
	 * M&#233;todo que realiza la operaci&#243;n de cruce de punto doble 
	 * sobre dos grupos de individuos.
	 * @return Los grupos de individuos cruzados aleatoriamente.
	 */
	public abstract String[][] crossOver(Hashtable parameters, Hashtable problemConfigParameters, String[][]crossArray);
	/**
	 * M&#233;todo que implementa la operaci&#243;n de mutaci&#243;n sobre un individuo concreto,
	 * en funci&#243;n de una probabilidad.
	 * @param Individuo a mutar.
	 * @return Individuo mutado.
	 */
	public abstract String mutate(Hashtable parameters, Hashtable problemConfigParameters,String individual);
}
