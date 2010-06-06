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
 * Clase que leer&#224; los datos de entrada del cliente y ejecutar&#224; las iteraciones 
 * recibidas hasta que encuentre un resultado apropiado o se agoten dichas iteraciones.
 * @author Alberto Luengo Cabanillas
 * @since 1.0
 *
 */
public class Coordinador implements ICoordinador {

	Path localPopulationFile;
	String USERNAME;
	String hdfsPopString;
	Path hdfsPopulationPath;
	String subOptString; 
	Path subOptimalResultsFilePath;
	Hashtable<String, Integer> hTable;
	

	/**
	 * M&#233;todo constructor de la clase <code>Coordinador</code>
	 * @param userName Nombre de usuario con permisos suficientes que lanza 
	 * el trabajo MapReduce.
	 */
	Coordinador(String userName) {
		localPopulationFile=new Path("./population.dat");
		USERNAME = userName;
		hdfsPopString = "/user/"+USERNAME+"/input/population.dat";
		hdfsPopulationPath=new Path(hdfsPopString);
		subOptString = "/user/"+USERNAME+"/output/part-r-00000";
		subOptimalResultsFilePath= new Path(subOptString);
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
	
	
	
	@SuppressWarnings("static-access")
	@Override
	public String readFromClientAndIterate(int numPop, int maxiter, int debug, int boolElit, String numProblem, int endCriterial, int gene_number) throws IOException, ExecException, Exception {
		
		String bestIndividual="";
		String []args = new String[2];
		String []args2 = new String[2];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		JobContext jCont = new JobContext(conf, null);	
		
		
		//Lo primero que tiene hacer el Coordinador es regenerar la estructura de dirs...
		this.regenDirs(fs);
		
		String oldPopString = "/user/"+USERNAME+"/oldPopulations";
		Path oldPopulationsDirPath = new Path (oldPopString);
		
		
		//Simulamos el criterio de fin de ejecución por consecución del objetivo con un numero de iteraciones muy elevado
		if (endCriterial == 1) {
			maxiter = 1000000;
		}
			
		for (int i=0; i<maxiter; i++) 
		{
			
			/**Si es la primera iteracion, subiremos la poblacion inicial, sino la de los
			 * descendientes. Si no es la primera iteracion tendra que ejecutar el codigo Pig para
		     * saber cual es la poblacion optima de la iteracion, almacenandola ya en el master... 
			 */
			String currPopString = "/user/"+USERNAME+"/input/population_"+i+".dat";
			Path currentPopulationFilePath = new Path (currPopString);
			
			//Si es la primera iteracion, leemos el fichero localmente...
			if (i==0) this.uploadToHDFS(jCont, localPopulationFile.toString(),boolElit);		
			
			System.out.println("COORDINADOR["+i+"]: SE LLAMA AL MASTER");
			//Le paso los argumentos...
			args[0] = numProblem;
			String iteration=i+"";
			args[1] = iteration;
			MRPGAMaster.main(args);
			System.out.println("COORDINADOR["+i+"]: ACABA EL MASTER");
			
			/**Miramos si en la poblacion resultante tenemos el resultado objetivo... 
			 */
			System.out.println("COORDINADOR["+i+"]: BUSCO EL FITNESS OBJETIVO...");			
			hTable = this.generateIndividualsTable(subOptimalResultsFilePath,numProblem);
			/**
			 * Si el problema es el de 'frase objetivo',
			 * su fitness ideal sera 0
			 */
			if (numProblem == "1")
				if (hTable.containsValue(0))
				{
					System.out.println("COORDINADOR[TARGETPHRASE]: EN LA ITERACIÓN "+i+" SE HA ENCONTRADO EL INDIVIDUO OBJETIVO");
					break;
				}
					
				else 
					System.out.println("COORDINADOR[TARGETPHRASE]: EN LA ITERACION "+i+" NO SE ENCUENTRA EL INDIVIDUO OBJETIVO");
			
			/**
			 * Si el problema es 'OneMax', su fitness ideal 
			 * sera la longitud total del individuo a 1
			 */
			if (numProblem == "2")
				if (hTable.containsValue(gene_number))
				{
					System.out.println("COORDINADOR[ONEMAX]: EN LA ITERACIÓN "+i+" SE HA ENCONTRADO EL INDIVIDUO OBJETIVO");
					break;
				}
				else 
					System.out.println("COORDINADOR[ONEMAX]: EN LA ITERACION "+i+" NO SE ENCUENTRA EL INDIVIDUO OBJETIVO");
			
			/**
			 * Si el problema es 'PPeaks', 
			 * su fitness ideal sera 1 (el individuo mas cercano a un pico dado)...
			 */
			if (numProblem == "3")
				if (hTable.containsValue(1))
				{
					System.out.println("COORDINADOR[PPEAKS]: EN LA ITERACIÓN "+i+" SE HA ENCONTRADO EL INDIVIDUO OBJETIVO");
					break;
				}
				else 
					System.out.println("COORDINADOR[PPEAKS]: EN LA ITERACION "+i+" NO SE ENCUENTRA EL INDIVIDUO OBJETIVO");
			
			System.out.println("COORDINADOR["+i+"]: Comienza el script de Pig");
			PigFunction pigFun = new PigFunction();
			args2[0] = subOptimalResultsFilePath.toString();
			args2[1] = i+"";
			pigFun.main(args2);
			System.out.println("COORDINADOR["+i+"]: Acaba el script de Pig");
			
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
		}
		//Imprimimos el(los) mejor(es) individuo(s) que hayamos encontrado...
		System.out.println("COORDINADOR: IMPRESIÓN DEL(DE LOS) MEJOR(ES) INDIVIDUO(S)...");
		bestIndividual = printBestIndividual(hTable,numProblem);
		//System.out.println("COORDINADOR: Acabo de imprimir el mejor individuo...");
	return bestIndividual;
	}

	
	@Override
	public void replacePopulationFile(Path originalPop, Path actualPopPath) throws IOException {
		//
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		//Path populationPath = new Path("input/population.txt");
		//System.out.println("COORDINADOR: Dentro del replaceFile el originalPop es "+originalPop+" y el actualPop es "+actualPopPath);
		
	    try {
	    	if (fs.exists(originalPop)) {
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
	public void uploadToHDFS(JobContext cont, String population, int boolElit) throws IOException {
		//Indicamos a que directorio del HDFS lo queremos subir...  
		final String HDFS_POPULATION_FILE= "/user/"+USERNAME+"/input/population.dat";
		
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
		
		//Si el elitismo esta presente, creamos el directorio para ir almacenando los mejores individuos de cada iteracion...
		if (boolElit == 1)
		{
			String bestIndString = "/user/"+USERNAME+"/bestIndividuals";
			fs.mkdirs(new Path(bestIndString));
		}
		
		//Mandamos el fichero de configuracion a todos los nodos...
		//DistributedCache.addCacheFile(hdfsConfMapPath.toUri(),cont.getConfiguration());
		//DistributedCache.addCacheFile(hdfsConfRedPath.toUri(),cont.getConfiguration());
	}

	@Override
	public String printBestIndividual(Hashtable hashTable, String numProblem) {
		Hashtable bestTable = new Hashtable();
		Enumeration claves = hashTable.keys();
		//System.out.println("CLAVES VALE "+claves);
		int fitValue = 0, bestFitness = 0;
		float fitValFloat = 0, bestFitFloat = 0;
		
		//Inicializamos el fitness al del primer individuo...
		if ((Integer.parseInt(numProblem) == 1)||(Integer.parseInt(numProblem) == 2))
			bestFitness = (int)Double.parseDouble(hashTable.elements().nextElement().toString());
		else
			bestFitFloat = Float.parseFloat(hashTable.elements().nextElement().toString());
		
		while (claves.hasMoreElements())
		{
			String clave = (String)claves.nextElement();
			//System.out.println("LA CLAVE ES "+clave);
			if ((Integer.parseInt(numProblem)== 1)||(Integer.parseInt(numProblem)== 2))
			{
				fitValue = (int)Double.parseDouble(hashTable.get(clave).toString());
				//System.out.println("EL FITVALUE ES "+fitValue);
			}
			else
				fitValFloat = Float.parseFloat(hashTable.get(clave).toString());
			
			//System.out.println("EL VALOR TAL CUAL ES "+hashTable.get(clave).toString());
			//System.out.println("EL FITVALFLOAT ES "+fitValFloat);
			//System.out.println("EL FITVALDOUBLE ES "+Double.parseDouble(hashTable.get(clave).toString()));
				
			/**
			 * Si es el problema 'frase objetivo' 
			 * el mejor fitness sera el m&#224;s pequeño...
			 */
			if (Integer.parseInt(numProblem) == 1)
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
			 * el mejor fitness sera el m&#224;s alto...
			 */
			else 
				if (Integer.parseInt(numProblem) == 3)
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
		String result ="Se ha producido algún error previo a la impresión de individuos...";
		if (bestTable.size() == 1)
			result = "El mejor individuo encontrado es: "+bestTable;
		else
			result = "Los mejores individuos encontrados son: "+bestTable;
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
				if (Integer.parseInt(numProblem)!= 3)
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
