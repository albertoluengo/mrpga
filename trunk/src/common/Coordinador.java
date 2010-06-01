package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;


public class Coordinador implements ICoordinador {

	/**
	 * Leera los datos de entrada del cliente y ejecutara las iteraciones recibidas
	 * hasta que encuentre un resultado apropiado...
	 * @throws IOException 
	 * @throws ExecException 
	 * @throws Exception
	 */
	Path localPopulationFile;
	String USERNAME;
	String hdfsPopString;
	Path hdfsPopulationPath;
	String subOptString; 
	Path subOptimalResultsFilePath;
	//Path pigResultFile= new Path("/user/hadoop-user/output/pigResults/part-00000");
	Hashtable<String, Integer> hTable;
	
	//Inicializo las variables en el constructor...
	Coordinador() {
		localPopulationFile=new Path("./population.txt");
		USERNAME = this.getUserName();
		hdfsPopString = "/user/"+USERNAME+"/input/population.txt";
		hdfsPopulationPath=new Path(hdfsPopString);
		subOptString = "/user/"+USERNAME+"/output/part-r-00000";
		subOptimalResultsFilePath= new Path(subOptString);
		hTable = new Hashtable();
	}
	
	private String getUserName() {
		Configuration conf = new Configuration();
		String users = conf.get("hadoop.job.ugi");
		String[] commas = users.split(",");
		String userName = commas[0];
		return userName;
	}
	
	private void regenDirs(FileSystem fs) throws IOException {
		Path outputPath = new Path("output");
		Path inputPath = new Path("input");
		Path dataPath = new Path("data");
		Path bestPath = new Path("bestIndividuals");
		Path oldPath = new Path("oldPopulations");
	   
	    try {
	    	if (fs.exists(outputPath)) 
	    		//Eliminamos el directorio de salida primero...
	    		fs.delete(outputPath,true);
	    	}
	    catch (IOException ioe) {
	    	System.err.println("COORD:Se ha producido un error borrando el dir de salida!");
	    	System.exit(1);
	    }
	    
	    try {
	    	if (fs.exists(inputPath)) 
	    		//Eliminamos el directorio de salida primero...
	    		fs.delete(inputPath,true);
	    	}
	    catch (IOException ioe) {
	    	System.err.println("COORD:Se ha producido un error borrando el dir de entrada!");
	    	System.exit(1);
	    }
	    
	    try {
	    	if (fs.exists(dataPath)) 
	    		//Eliminamos el directorio de salida primero...
	    		fs.delete(dataPath,true);
	    	}
	    catch (IOException ioe) {
	    	System.err.println("COORD:Se ha producido un error borrando el dir de data!");
	    	System.exit(1);
	    }
	    
	    try {
	    	if (fs.exists(bestPath)) 
	    		//Eliminamos el directorio de salida primero...
	    		fs.delete(bestPath,true);
	    	}
	    catch (IOException ioe) {
	    	System.err.println("COORD:Se ha producido un error borrando el dir de bestInd!");
	    	System.exit(1);
	    }
	    
	    try {
	    	if (fs.exists(oldPath)) 
	    		//Eliminamos el directorio de salida primero...
	    		fs.delete(oldPath,true);
	    	}
	    catch (IOException ioe) {
	    	System.err.println("COORD:Se ha producido un error borrando el dir de oldPops!");
	    	System.exit(1);
	    }
	}
	
	
	
	@Override
	public String readFromClientAndIterate(int numPop, int maxiter, int debug, int boolElit, String numProblem, int endCriterial, int gene_number) throws IOException, ExecException, Exception {
		
		String bestIndividual="";
		String []args = new String[2];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		JobContext jCont = new JobContext(conf, null);	
		
		
		//Lo primero que tiene hacer el Coordinador es regenerar la estructura de dirs...
		this.regenDirs(fs);
		
		String oldPopString = "/user/"+USERNAME+"/oldPopulations";
		Path oldPopulationsDirPath = new Path (oldPopString);
		
		
		//Simulamos el criterio de fin de ejecución por consecución del objetivo con un numero de iteraciones muy elevado
		if (endCriterial == 1) {
			maxiter = 100000;
		}
			
		for (int i=0; i<maxiter; i++) 
		{
			
			/**Si es la primera iteracion, subiremos la poblacion inicial, sino la de los
			 * descendientes. Si no es la primera iteracion tendra que ejecutar el codigo Pig para
		     * saber cual es la poblacion optima de la iteracion, almacenandola ya en el master... 
			 */
			String currPopString = "/user/"+USERNAME+"/input/population_"+i+".txt";
			Path currentPopulationFilePath = new Path (currPopString);
			
			//Si es la primera iteracion, leemos el fichero localmente...
			if (i==0) this.uploadToHDFS(jCont, localPopulationFile.toString());		
			
			System.out.println("COORDINADOR["+i+"]: Llamo al master");
			//Le paso los argumentos...
			args[0] = numProblem;
			String iteration=i+"";
			args[1] = iteration;
			MRPGAMaster.main(args);
			System.out.println("COORDINADOR["+i+"]: Acaba el master");
			
			/**Miramos si en la poblacion resultante tenemos el resultado objetivo... 
			 */
			System.out.println("COORDINADOR["+i+"]: BUSCO EL INDIVIDUO OBJETIVO...");			
			hTable = this.generateIndividualsTable(subOptimalResultsFilePath,numProblem);
			
			/**
			 * Si el problema es el de 'frase objetivo',
			 * su fitness ideal sera 0
			 */
			if (numProblem == "1")
				if (hTable.containsValue(0)) 
					break;
				else 
					System.out.println("COORDINADOR["+i+"]: EN LA ITERACION "+i+" NO ENCUENTRO EL INDIVIDUO OBJETIVO");
			
			/**
			 * Si el problema es 'OneMax', su fitness ideal 
			 * sera la longitud total del individuo a 1
			 */
			if (numProblem == "2")
				if (hTable.containsValue(gene_number)) 
					break;
				else 
					System.out.println("COORDINADOR["+i+"]: EN LA ITERACION "+i+" NO ENCUENTRO EL INDIVIDUO OBJETIVO");
			
			/**
			 * Si el problema es 'PPeaks', 
			 * su fitness ideal sera 1 (el individuo mas cercano a un pico dado)...
			 */
			if (numProblem == "3")
				if (hTable.containsValue(1)) 
					break;
				else 
					System.out.println("COORDINADOR["+i+"]: EN LA ITERACION "+i+" NO ENCUENTRO EL INDIVIDUO OBJETIVO");
			
			System.out.println("COORDINADOR["+i+"]: Llamo al script de Pig");		
			this.runPigScript(subOptimalResultsFilePath.toString(),i,conf);
			System.out.println("COORDINADOR["+i+"]: Acaba el script de Pig");
			
			//Si el parámetro "debug" está activado, vamos a crear un directorio nuevo en el que se van a ir colocando
			//todas las poblaciones, para poder ver su evolución...
			if (debug==1) 
			{
				fs.mkdirs(oldPopulationsDirPath);
				String targetString = "/user/"+USERNAME+"/oldPopulations/population_"+i+".txt"; 
				Path targetFilePopPath = new Path (targetString);
				if (i==0) {
					//Movemos la poblacion inicial y la que obtiene Pig
					String initString = "/user/"+USERNAME+"/oldPopulations/population.txt"; 
					Path initialFilePopPath =  new Path(initString);
					fs.rename(hdfsPopulationPath, initialFilePopPath);
					//Necesitamos una copia de la última población descendiente que sirva de entrada para la siguiente iteracion
					FileUtil.copy(fs, currentPopulationFilePath, fs, targetFilePopPath, false, conf);	
				}
					
				else {
					//Borramos el fichero de poblacion de la descendencia anterior...
					String prevPopString = "/user/"+USERNAME+"/input/population_"+(i-1)+".txt"; 
					fs.delete(new Path(prevPopString), true);
					//Copiamos el fichero como entrada de la siguiente iteracion...
					FileUtil.copy(fs, currentPopulationFilePath, fs, targetFilePopPath, false, conf);
				}
			}	
		}
		//Si no se introduce elitismo, imprimimos el(los) mejor(es) individuo(s) que hayamos encontrado...
		System.out.println("COORDINADOR: Imprimo el mejor individuo...");
		bestIndividual = printBestIndividual(hTable,numProblem);
		System.out.println("COORDINADOR: Acabo de imprimir el mejor individuo...");
	return bestIndividual;
	}

	/**
	 * Este metodo ejecuta codigo Pig Latin embebido en Java, de tal forma que recopile los
	 * distintos individuos sub-optimos que generen los "reduce" locales (que estaran almacenados
	 * en un formato de documento de texto) y aplique sobre ellos las operaciones que necesitemos
	 * (merge, order, filter y select -en principio...). Ademas le tendra que enviar el fichero
	 * resultante con la poblacion optima de la iteracion al Coordinador, para que este se la 
	 * pase al master y comience una nueva iteracion
	 * @author Alberto Luengo Cabanillas
	 *
	 */
	@Override
	public void runPigScript(String inputFile, int iteration, Configuration conf) throws ExecException, IOException {
		
		//Tenemos que leer un fichero del HDFS
		System.out.println("COORDINADOR["+iteration+"]: Dentro del script de Pig");
		FileSystem fs = FileSystem.get(conf);
		Path resultPath = new Path("pigResults");
	    
	    try {
	    	if (fs.exists(resultPath)) {
	    		//Borro el directorio con todo su contenido
	    		fs.delete(resultPath,true);
	    	}
	    }
	    catch (IOException ioe) {
	    	System.err.println("COORDINADOR["+iteration+"]:Se ha producido un error borrando el dir de salida de Pig");
	    	System.exit(1);
	    }
		
	    String popIterationName = "input/population_"+iteration+".txt";
		
		PigServer pigServer = new PigServer("mapreduce");
		
		//PigContext pigContext = pigServer.getPigContext();
		//String jobName = "pigPopulation";
		//pigServer.getPigContext().getProperties().setProperty(PigContext.JOB_NAME,jobName);
		pigServer.registerQuery("raw_data = load '" + inputFile + "' using PigStorage('	');");
		pigServer.registerQuery("B = foreach raw_data generate $0 as id;");
		pigServer.registerQuery("store B into 'pigResults';");
		pigServer.renameFile("pigResults/part-00000", popIterationName);
		
		//Cuando acabo, borro el contenido del dir 'pigResults' (en el que sólo quedarán los logs...)
		fs.delete(resultPath,true);
		
		//pigServer.registerQuery("grouped = group raw_data by $0;");
		//pigServer.registerQuery("data = foreach grouped generate FLATTEN(group) as value;");
		//pigServer.registerQuery("full = foreach A generate $0 as id;");
		
	}
	@Override
	public void replacePopulationFile(Path originalPop, Path actualPopPath) throws IOException {
		//Leemos el fichero de poblacion que tenemos en el HDFS y lo reemplazamos
		//por el de la descendencia antes de entrar en la siguiente iteracion
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		//Path populationPath = new Path("input/population.txt");
		//System.out.println("COORDINADOR: Dentro del replaceFile el originalPop es "+originalPop+" y el actualPop es "+actualPopPath);
		
	    try {
	    	if (fs.exists(originalPop)) {
	    		System.out.println("Dentro del replaceFile: Sí existe el originalPop...");
	    		//remove the file first
	    		fs.delete(originalPop,true);
	    		fs.rename(actualPopPath, hdfsPopulationPath);
	    	}
	    }
	    catch (IOException ioe) {
	    	System.err.println("COORDINADOR: Se ha producido un error reemplazando los ficheros");
	    	System.exit(1);
	    }
	}

	@Override
	public void uploadToHDFS(JobContext cont, String population) throws IOException {
		//Indicamos a que directorio del HDFS lo queremos subir...  
		final String HDFS_POPULATION_FILE= "/user/"+USERNAME+"/input/population.txt";
		
		//Hacemos lo mismo con los ficheros de configuracion para los nodos worker...
		final String LOCAL_MAPPER_CONFIGURATION_FILE="./mapper_configuration.dat";
		//Indicamos a que directorio del HDFS lo queremos subir...  
		final String HDFS_MAPPER_CONFIGURATION_FILE= "/user/"+USERNAME+"/data/mapper_configuration.dat";

		final String LOCAL_REDUCER_CONFIGURATION_FILE="./reducer_configuration.dat";
		//Indicamos a que directorio del HDFS lo queremos subir...
		final String HDFS_REDUCER_CONFIGURATION_FILE="/user/"+USERNAME+"/data/reducer_configuration.dat";
		
		FileSystem fs = FileSystem.get(cont.getConfiguration());
		
		Path hdfsPopPath = new Path(HDFS_POPULATION_FILE);
		Path hdfsConfMapPath = new Path(HDFS_MAPPER_CONFIGURATION_FILE);
		Path hdfsConfRedPath = new Path(HDFS_REDUCER_CONFIGURATION_FILE);
		
		//subimos el fichero al HDFS del nodo master. Sobreescribimos cualquier copia.
		fs.copyFromLocalFile(false, true, new Path(population), hdfsPopPath);
		//Hacemos lo mismo con los ficheros de configuracion para poder distribuirlos...
		fs.copyFromLocalFile(false, true, new Path(LOCAL_MAPPER_CONFIGURATION_FILE), hdfsConfMapPath);
		fs.copyFromLocalFile(false, true, new Path(LOCAL_REDUCER_CONFIGURATION_FILE), hdfsConfRedPath);
		
		//Creamos el directorio para ir almacenando los mejores individuos de cada iteracion...
		String bestIndString = "/user/"+USERNAME+"/bestIndividuals";
		fs.mkdirs(new Path(bestIndString));
		
		//Mandamos el fichero de configuracion a todos los nodos...
		//DistributedCache.addCacheFile(hdfsConfMapPath.toUri(),cont.getConfiguration());
		//DistributedCache.addCacheFile(hdfsConfRedPath.toUri(),cont.getConfiguration());
	}

	@Override
	public String printBestIndividual(Hashtable hashTable, String numProblem) {
		Hashtable bestTable = new Hashtable();
		Enumeration claves = hashTable.keys();
		int fitValue = 0, bestFitness = 0;
		float fitValFloat = 0, bestFitFloat = 0;
		//Inicializamos el fitness al del primer individuo...
		if (numProblem !="3")
			bestFitness = Integer.parseInt(hashTable.elements().nextElement().toString());
		else
			bestFitFloat = Float.parseFloat(hashTable.elements().nextElement().toString());
		while (claves.hasMoreElements())
		{
			String clave = (String)claves.nextElement();
			//System.out.println("LA CLAVE ES "+clave);
			if (numProblem !="3")
			{
				fitValue = Integer.parseInt(hashTable.get(clave).toString());
				//System.out.println("EL FITVALUE ES "+fitValue);
			}
			else
				fitValFloat = Float.parseFloat(hashTable.get(clave).toString());
			
				
			/**
			 * Si es el problema 'frase objetivo' 
			 * el mejor fitness sera el más pequeño...
			 */
			if (numProblem == "1")
			{
				if (fitValue < bestFitness)
				{
					bestTable.clear();
					bestTable.put(clave, fitValue);
					bestFitness = fitValue;					
				}
				else if (fitValue == bestFitness)
					bestTable.put(clave, fitValue);
			}
			
			/**
			 * El resto de los problemas 
			 * el mejor fitness sera el más alto...
			 */
			else 
				if (numProblem == "3")
				{
					if (fitValFloat > bestFitFloat)
					{
						bestTable.clear();
						bestTable.put(clave, fitValFloat);
						bestFitFloat = fitValFloat;					
					}
					else if (fitValFloat == bestFitFloat)
						bestTable.put(clave, fitValFloat);
				}
				else
				{
					if (fitValue > bestFitness)
					{
						bestTable.clear();
						bestTable.put(clave, fitValue);
						bestFitness = fitValue;					
					}
					else if (fitValue == bestFitness)
						bestTable.put(clave, fitValue);
				}
		}
		String result = "Best individual(s) found is (are): "+bestTable;
		return result;
	}
	
	@Override
	public String readFromHDFS(String stringPath) {
		Path pathToRead = new Path(stringPath);
		FileSystem hdfs;
		String strLine = "", bestIndividual = "", result = "";
		try {
			hdfs = FileSystem.get(new Configuration());
			//Validamos primero el path de entrada antes de leer del fichero
			if (!hdfs.exists(pathToRead))
			{
				throw new IOException("El fichero especificado " +pathToRead.toString() + "no existe");
			}
			
			if (!hdfs.isFile(pathToRead))
			{
				throw new IOException("El fichero especificado "+pathToRead.toString() + "no existe");
			}
			FSDataInputStream dis = hdfs.open(pathToRead);
			BufferedReader br = new BufferedReader(new InputStreamReader(dis));
			
			while ((strLine = br.readLine()) != null)   {
				bestIndividual = strLine;
		      }
			dis.close();
			result = "Best individual: "+bestIndividual;
		
		} catch (IOException e) {
			result="ERROR READING FILE FROM HDFS:IOEXCEPTION";
		}
		return result;
	}

	@Override
	public Hashtable generateIndividualsTable(Path resultsPath, String numProblem) throws IOException {
		Hashtable hTable = new Hashtable();
		FileSystem hdfs = FileSystem.get(new Configuration());
		Scanner s = null;
	
		//Leo el fichero alojado en el HDFS
		//Validamos primero el path de entrada antes de leer del fichero
		if (!hdfs.exists(resultsPath))
		{
			throw new IOException("El fichero especificado " +resultsPath.toString() + "no existe");
		}
		
		if (!hdfs.isFile(resultsPath))
		{
			throw new IOException("El fichero especificado "+resultsPath.toString() + "no existe");
		}
		
		FSDataInputStream dis = hdfs.open(resultsPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(dis));
		
	    try {
	    	s = new Scanner(br);
			while (s.hasNextLine()) {
				String linea = s.nextLine();
				Scanner sl = new Scanner(linea);
				/**La expresion regular que nos indica que nuestro delimitador es uno o
				 * varios espacios es \\s.
				 */
				sl.useDelimiter("\t");
				/**Ahora metemos el primer elemento que encontramos (la palabra) como
				 * clave del Hashtable y el segundo (el fitness) como valor
				 */
				String keyWord = sl.next();
				String fitness = sl.next();
				/**
				 * En el caso del problema de los P-Picos, manejamos siempre
				 * valores double...
				 */
				double valor = Double.parseDouble(fitness);
				if (numProblem != "3")
					valor = (int)valor;
				hTable.put(keyWord, valor);
			}
	    }
	    catch(Exception e){
	    	e.printStackTrace();
	    }finally{
	    	/**En el finally cerramos el fichero, para asegurarnos
	    	 * que se cierra tanto si todo va bien como si salta 
	    	 * una excepcion.
	    	 */
	    	try{                    
	    		if( null != s ){   
	    			s.close();     
	    		}                  
	    	}catch (Exception e2){ 
	    		e2.printStackTrace();
	    	}
	    }
	return hTable;
	}
}
