package common;

import java.io.BufferedReader;
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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.backend.executionengine.ExecException;



/**
 * Clase que leer&#225; los datos de entrada del cliente y ejecutar&#225; las iteraciones 
 * recibidas hasta que encuentre un resultado apropiado o se agoten dichas iteraciones.
 * @author Alberto Luengo Cabanillas
 *
 */
public class Coordinador implements ICoordinador {

	Path localPopulationFile;
	String USERNAME;
	String hdfsPopString;
	Path hdfsPopulationPath;
	String subOptString; 
	Hashtable<String, Integer> hTable;
	

	/**
	 * M&#233;todo constructor de la clase <code>Coordinador</code>
	 * @param userName Nombre de usuario con permisos suficientes que lanza 
	 * el trabajo MapReduce.
	 */
	Coordinador(String userName, String userDir) {
		USERNAME = userName;
		localPopulationFile=new Path(userDir+USERNAME+"/population.dat");
		hdfsPopString = "/user/"+USERNAME+"/input/population.dat";
		hdfsPopulationPath=new Path(hdfsPopString);
		hTable = new Hashtable();
	}
		
	
	/**
	 * M&#233;todo privado que se ocupa de borrar todos los directorios creados
	 * durante las distintas ejecuciones del sistema.
	 * @param fs Instancia de la clase <code>FileSystem</code> que representa
	 * el sistema de ficheros <code>HDFS</code>.
	 * @throws IOException Excepci&#243;n lanzada al haber alg&#250;n problema manipulando
	 * ficheros o directorios.
	 */
	private void regenIODirs(FileSystem fs) throws IOException {
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
	    	System.err.println("COORD:Se aborta la ejecución...");
	    	System.exit(1);
	    }
	    
	    try {
	    	if (fs.exists(inputPath)) 
	    		//Eliminamos el directorio de salida primero...
	    		fs.delete(inputPath,true);
	    	}
	    catch (IOException ioe) {
	    	System.err.println("COORD:Se ha producido un error borrando el dir de entrada!");
	    	System.err.println("COORD:Se aborta la ejecución...");
	    	System.exit(1);
	    }
	    
	    try {
	    	if (fs.exists(dataPath)) 
	    		//Eliminamos el directorio de salida primero...
	    		fs.delete(dataPath,true);
	    	}
	    catch (IOException ioe) {
	    	System.err.println("COORD:Se ha producido un error borrando el dir de data!");
	    	System.err.println("COORD:Se aborta la ejecución...");
	    	System.exit(1);
	    }
	    
	    try {
	    	if (fs.exists(bestPath)) 
	    		//Eliminamos el directorio de salida primero...
	    		fs.delete(bestPath,true);
	    	}
	    catch (IOException ioe) {
	    	System.err.println("COORD:Se ha producido un error borrando el dir de bestInd!");
	    	System.err.println("COORD:Se aborta la ejecución...");
	    	System.exit(1);
	    }
	    
	    try {
	    	if (fs.exists(oldPath)) 
	    		//Eliminamos el directorio de salida primero...
	    		fs.delete(oldPath,true);
	    	}
	    catch (IOException ioe) {
	    	System.err.println("COORD:Se ha producido un error borrando el dir de oldPops!");
	    	System.err.println("COORD:Se aborta la ejecución...");
	    	System.exit(1);
	    }    
	    
	}
	
	@Override
	public String readFromClientAndIterate(int numPop, int numReducers, int maxiter, int debug, int boolElit, String problemName, String reducerName, int endCriterial, int gene_number, Hashtable configValues, String userDir) throws IOException, ExecException, Exception {
		
		String bestIndividual="";
		String []args = new String[4];
		String []args2 = new String[3];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		JobContext jCont = new JobContext(conf, null);	
		String targetFitnessString = configValues.get("targetFitness").toString();
		double targetFitness =Double.parseDouble(targetFitnessString);
		String bestCritFitness = configValues.get("bestFitness").toString();
		
		//Lo primero que tiene hacer el Coordinador es regenerar la estructura de dirs...
		this.regenIODirs(fs);
		
		String oldPopString = "/user/"+USERNAME+"/oldPopulations";
		Path oldPopulationsDirPath = new Path (oldPopString);
		
		
		//Simulamos el criterio de fin de ejecución por consecución del objetivo con un numero de iteraciones muy elevado
		if (endCriterial == 1) {
			maxiter = 100000000;
		}
			
		for (int i=0; i<maxiter; i++) 
		{
			final long startTime = System.currentTimeMillis();
			/**Si es la primera iteracion, subiremos la poblacion inicial, sino la de los
			 * descendientes */
			String currPopString = "/user/"+USERNAME+"/input/population_"+i+".dat";
			Path currentPopulationFilePath = new Path (currPopString);
			
			//Si es la primera iteracion, leemos el fichero localmente...
			if (i==0) this.uploadToHDFS(jCont, localPopulationFile.toString(),boolElit,userDir);		
			
			//Instancio dinámicamente las clases necesarias para el trabajo MapReduce...
			Class <? extends MRPGAMapper> problem_map_class;
			Class <? extends MRPGAReducer> problem_reducer_class;
			String map_class_name = "problems."+problemName;
			String reduce_class_name = "problems."+reducerName;
			problem_map_class = (Class <? extends MRPGAMapper>)Class.forName(map_class_name);
			problem_reducer_class = (Class <? extends MRPGAReducer>)Class.forName(reduce_class_name);
			
			String iteration=i+"";
			args[0] = iteration;
			String numRed=numReducers+"";
			args[1] = numRed;
			args[2] = problem_map_class.getName().toString();
			args[3] = problem_reducer_class.getName().toString();
			System.out.println("COORDINADOR["+i+"]: SE LLAMA AL MASTER");
			MRPGAMaster.main(args);
			System.out.println("COORDINADOR["+i+"]: ACABA EL MASTER");
			
			/**Miramos si en la poblacion resultante tenemos el resultado objetivo... 
			 */
			System.out.println("COORDINADOR["+i+"]: BUSCO EL FITNESS OBJETIVO...");			
			hTable = this.generateIndividualsTable(numReducers);
			
			if (hTable.containsValue(targetFitness))
			{
				System.out.println("COORDINADOR["+problemName+"]: EN LA ITERACIÓN "+i+" SE HA ENCONTRADO EL INDIVIDUO OBJETIVO");
				break;
			}
			else 
				System.out.println("COORDINADOR["+problemName+"]: EN LA ITERACION "+i+" NO SE ENCUENTRA EL INDIVIDUO OBJETIVO");
			
			
			System.out.println("COORDINADOR["+i+"]: COMIENZA EL SCRIPT DE PIG");
			PigNode pigFun = new PigNode();
			args2[0] = i+"";
			args2[1] = numReducers+"";
			args2[2] = USERNAME;
			pigFun.main(args2);
			System.out.println("COORDINADOR["+i+"]: ACABA EL SCRIPT DE PIG");
			
			/**
			 * Si el par&#224;metro "debug" est&#224; activado, vamos a crear un directorio nuevo en el que se van a ir colocando
			 * todas las poblaciones, para poder ver su evolución...
			 */
			//
			if (debug==1) 
			{
				fs.mkdirs(oldPopulationsDirPath);
				String targetString = "/user/"+USERNAME+"/oldPopulations/population_"+i+".dat"; 
				Path targetFilePopPath = new Path (targetString);
				if (i==0) {
					//Movemos la poblacion inicial y la que obtiene Pig
					String initString = "/user/"+USERNAME+"/oldPopulations/population.dat"; 
					Path initialFilePopPath =  new Path(initString);
					fs.rename(hdfsPopulationPath, initialFilePopPath);
					//Necesitamos una copia de la última población descendiente que sirva de entrada para la siguiente iteracion
					FileUtil.copy(fs, currentPopulationFilePath, fs, targetFilePopPath, false, conf);	
				}
					
				else {
					//Borramos el fichero de poblacion de la descendencia anterior...
					String prevPopString = "/user/"+USERNAME+"/input/population_"+(i-1)+".dat"; 
					fs.delete(new Path(prevPopString), true);
					//Copiamos el fichero como entrada de la siguiente iteracion...
					FileUtil.copy(fs, currentPopulationFilePath, fs, targetFilePopPath, false, conf);
				}
			}
			//Si no se guarda el historico de poblaciones, las vamos eliminando... 
			else
			{
				if (i == 0)
					//Borramos el fichero de poblacion original...
					fs.delete(hdfsPopulationPath, true);
				else
				{
					//Borramos el fichero de poblacion de la descendencia anterior...
					String prevPopString = "/user/"+USERNAME+"/input/population_"+(i-1)+".dat"; 
					fs.delete(new Path(prevPopString), true);
				}
			}
			//Imprimimos el tiempo de la iteración...
			final double duration = (System.currentTimeMillis() - startTime)/1000.0;
		    System.out.println("COORDINADOR["+i+"]:TIEMPO DE LA ITERACIÓN " + duration + " segundos");
		}
		//Imprimimos el(los) mejor(es) individuo(s) que hayamos encontrado...
		System.out.println("COORDINADOR: IMPRESIÓN DEL(DE LOS) MEJOR(ES) INDIVIDUO(S)...");
		bestIndividual = printBestIndividual(hTable,bestCritFitness);
	return bestIndividual;
	}

	
//	@Override
//	public void replacePopulationFile(Path originalPop, Path actualPopPath) throws IOException {
//		//
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//		//Path populationPath = new Path("input/population.txt");
//		//System.out.println("COORDINADOR: Dentro del replaceFile el originalPop es "+originalPop+" y el actualPop es "+actualPopPath);
//		
//	    try {
//	    	if (fs.exists(originalPop)) {
//	    		//remove the file first
//	    		fs.delete(originalPop,true);
//	    		fs.rename(actualPopPath, hdfsPopulationPath);
//	    	}
//	    }
//	    catch (IOException ioe) {
//	    	System.err.println("COORDINADOR: Se ha producido un error reemplazando los ficheros");
//	    	System.exit(1);
//	    }
//	}

	
	@Override
	public void uploadToHDFS(JobContext cont, String population, int boolElit, String userDir) throws IOException {
		//Indicamos a que directorio del HDFS lo queremos subir...  
		final String HDFS_POPULATION_FILE= "/user/"+USERNAME+"/input/population.dat";
		
		//Hacemos lo mismo con los ficheros de configuracion para los nodos worker...
		final String LOCAL_GENERAL_PARAMS_FILE= userDir+USERNAME+"/general_params.dat";
		//Indicamos a que directorio del HDFS lo queremos subir...  
		final String HDFS_GENERAL_PARAMS_FILE= "/user/"+USERNAME+"/data/general_params.dat";

		final String LOCAL_PROBLEM_PARAMS_FILE= userDir+USERNAME+"/problem_params.dat";
		final String HDFS_PROBLEM_PARAMS_FILE="/user/"+USERNAME+"/data/problem_params.dat";
		
		FileSystem fs = FileSystem.get(cont.getConfiguration());
		
		Path hdfsPopPath = new Path(HDFS_POPULATION_FILE);
		Path hdfsGeneralParamsPath = new Path(HDFS_GENERAL_PARAMS_FILE);
		Path hdfsProblemParamsPath = new Path(HDFS_PROBLEM_PARAMS_FILE);
		
		//subimos el fichero al HDFS del nodo master. Sobreescribimos cualquier copia.
		fs.copyFromLocalFile(false, true, new Path(population), hdfsPopPath);
		//Hacemos lo mismo con los ficheros de configuracion para poder distribuirlos...
		fs.copyFromLocalFile(false, true, new Path(LOCAL_GENERAL_PARAMS_FILE), hdfsGeneralParamsPath);
		fs.copyFromLocalFile(false, true, new Path(LOCAL_PROBLEM_PARAMS_FILE), hdfsProblemParamsPath);
		
		//Si el elitismo esta presente, creamos el directorio para ir almacenando los mejores individuos de cada iteracion...
		if (boolElit == 1)
		{
			String bestIndString = "/user/"+USERNAME+"/bestIndividuals";
			fs.mkdirs(new Path(bestIndString));
		}
	}

	@Override
	public String printBestIndividual(Hashtable hashTable, String bestCritFitness) {
		Hashtable bestTable = new Hashtable();
		Enumeration claves = hashTable.keys();
		//System.out.println("CLAVES VALE "+claves);
		double fitValue = 0, bestFitness = 0;
		
		bestFitness = Double.parseDouble(hashTable.elements().nextElement().toString());
		
		while (claves.hasMoreElements())
		{
			String clave = (String)claves.nextElement();
			fitValue = Double.parseDouble(hashTable.get(clave).toString());
			
			/**
			 * Si el mejor fitness es el mas pequeño...
			 */
			if (bestCritFitness.equals("minor"))
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
			 * Si el mejor fitness es el mas alto...
			 */
			else { 
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
		String result ="COORDINADOR: Se ha producido algún error previo a la impresión de individuos...";
		if (bestTable.size() == 1)
			result = "COORDINADOR: El mejor individuo encontrado es: "+bestTable;
		else
			result = "COORDINADOR: Los mejores individuos encontrados son: "+bestTable;
		return result;
	}
	

	@Override
	public Hashtable generateIndividualsTable(int numReducers) throws IOException {
		Hashtable hTable = new Hashtable();
		FileSystem hdfs = FileSystem.get(new Configuration());
		Scanner s = null;
		String inputFiles = "output/part-r-0000";
		
		for (int numRed=0;numRed<numReducers; numRed++){
			//Leo el fichero alojado en el HDFS...
			//Validamos primero el path de entrada antes de leer del fichero
			Path resultPath = new Path(inputFiles+numRed);
			if (!hdfs.exists(resultPath))
			{
				throw new IOException("El fichero especificado " +resultPath.toString() + "no existe");
			}
			
			if (!hdfs.isFile(resultPath))
			{
				throw new IOException("El fichero especificado "+resultPath.toString() + "no existe");
			}
		
			FSDataInputStream dis = hdfs.open(resultPath);
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
		    	//Cerramos el descriptor de fichero
		    	dis.close();
		    }
		}
	return hTable;
	}
}
