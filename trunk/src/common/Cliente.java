package common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * Las funciones del cliente seran basicamente de introducir datos al sistema e inicializar
 * la poblacion a partir de esos datos:
 * 1.- Definir el tamanho de la poblacion
 * 2.- Definir el total de iteraciones
 * 3.- Definir el grado de elitismo en la descendencia
 * 4.- Definir el grado de mutacion
 * 5.- Definir cual es la frase objetivo
 * 
 */

public class Cliente extends Configured implements Tool {

	
	private static String generateRandomString(String target) {
			String individuo = "";
			int sizeTarget=target.length();
			Random r = new Random(System.nanoTime());
//			char spanish_chars[]={'!','¡','.','¿','?','_',',',';','á','é','í','ó','ú'};
//			Arrays.sort(spanish_chars);
			int i=0;
			//int position = 0;
				while ( i < sizeTarget){
					char c = (char)r.nextInt(255);
					//position = Arrays.binarySearch(spanish_chars, c);
					//position = 1;
					if((c>='0' && c<='9') || (c>='a' && c<='z') || (c>='A' && c<='Z'))
					{
						individuo += c;
						i ++;
					}
				}
			return individuo;
			}
		
	private static String generateRandomBinaryString(int geneNumber) {
		String cadenaAleatoria ="";
		int cont=0;
		double prob =0.0;
			while (cont < geneNumber)
			{
				prob = java.lang.Math.random();
				if ((prob <= 0.5))
					cadenaAleatoria += '0';
				else
					cadenaAleatoria += '1';
				cont ++;
			}
		return cadenaAleatoria;
		}
	
	
	private static void generatePopulationFile(String target, int sizePop, int geneNumber, int numProblem) {
		
		//Instanciamos el fichero...
		String sFile ="population.dat";
		File initPop = new File("./",sFile);
		//Miramos si existe...
		if (initPop.exists()){
			System.out.println("CLIENTE:Regenerando fichero de poblacion...");
			initPop.delete();
		}
		else
		{
			//Creamos el fichero....
			try {
				// A partir del objeto File creamos el fichero fisicamente
				if (initPop.createNewFile())
					System.out.println("CLIENTE:El fichero de poblacion ha sido creado correctamente!");
				else
					System.out.println("CLIENTE:El fichero de poblacion no ha podido ser creado...");
			}catch (IOException e){
				e.printStackTrace();
			}
		}
		//Escribimos en el fichero previamente creado...
		try {
			BufferedWriter bw= new BufferedWriter(new FileWriter(sFile));
			System.out.println("CLIENTE: Escribiendo en fichero de poblacion...");
			String word="";
			int i=0;
			while (i <sizePop) {
				if (numProblem == 1) //Caso del problem de la 'frase objetivo'
					//Generamos la palabra descendiente...
					word=generateRandomString(target);
				else
					//Generamos el individuo binario...
					word=generateRandomBinaryString(geneNumber);
				//Escribimos a fichero...	
				bw.write(word +"\n");
				i++;
			}
			//Cerramos el fichero
			System.out.println("CLIENTE: Cerrando fichero de poblacion...");
			bw.close();
		} catch (IOException e){e.printStackTrace();}
	}
	
	//Creamos el fichero de configuracion que debe subir el coordinador al HDFS para que el
	//Master lo distribuya entre los nodos trabajadores...
	private static void generateMapperConfigurationFile(String target, int numPop, int boolElit, int debug,int gene_length) {
		
		//Instanciamos el fichero...
		String sFile ="mapper_configuration.dat";
		File initPop = new File("./",sFile);
		//Miramos si existe...
		if (initPop.exists()){
			System.out.println("CLIENTE: Regenerando fichero de configuracion para Mappers...");
			initPop.delete();
		}
		else
		{
			//Creamos el fichero....
			try {
				// A partir del objeto File creamos el fichero fisicamente
				if (initPop.createNewFile())
					System.out.println("CLIENTE: El fichero de configuracion para Mappers ha sido creado correctamente!");
				else
					System.out.println("CLIENTE: El fichero de configuracion para Mappers no ha podido ser creado...");
			}catch (IOException e){
				e.printStackTrace();
			}
		}
		//Escribimos en el fichero previamente creado
		try {
			BufferedWriter bw= new BufferedWriter(new FileWriter(sFile));
			System.out.println("CLIENTE: Escribiendo en fichero de configuracion para Mappers...");
			bw.write(target +"\r\n");
			bw.write(numPop +"\r\n");
			bw.write(boolElit +"\r\n");
			bw.write(debug +"\r\n");
			bw.write(gene_length +"\r\n");
			
			//Cerramos el fichero
			System.out.println("CLIENTE: Cerrando fichero de configuracion para Mappers...");
			bw.close();
		} catch (IOException e){e.printStackTrace();}	
	}
	
	//Creamos el fichero de configuracion que debe subir el coordinador al HDFS para que el
	//Master lo distribuya entre los nodos trabajadores...
	private static void generateReducerConfigurationFile(int numpop, int maxiter, int boolElit,
			float mutationrate, int mutation, double crossProb, String target) {
		//Instanciamos el fichero...
		String sFile ="reducer_configuration.dat";
		File initPop = new File("./",sFile);
		//Miramos si existe...
		if (initPop.exists()){
			System.out.println("CLIENTE: Regenerando fichero de configuracion para Reducers...");
			initPop.delete();
		}
		else
		{
			//Creamos el fichero....
			try {
				// A partir del objeto File creamos el fichero fisicamente
				if (initPop.createNewFile())
					System.out.println("CLIENTE: El fichero de configuracion para Reducers ha sido creado correctamente!");
				else
					System.out.println("CLIENTE: El fichero de configuracion para Reducers no ha podido ser creado...");
			}catch (IOException e){
				e.printStackTrace();
			}
		}
		//Escribimos en el fichero previamente creado
		try {
			BufferedWriter bw= new BufferedWriter(new FileWriter(sFile));
			System.out.println("CLIENTE: Escribiendo en fichero de configuracion para Reducers...");
			bw.write(numpop +"\r\n");
			bw.write(maxiter +"\r\n");
			bw.write(boolElit +"\r\n");
			bw.write(mutationrate +"\r\n");
			bw.write(mutation +"\r\n");
			bw.write(crossProb +"\r\n");
			bw.write(target);
			//Cerramos el fichero
			System.out.println("CLIENTE: Cerrando fichero de configuracion para Reducers...");
			bw.close();
		} catch (IOException e){e.printStackTrace();}	
	}
	
	void launch(String numProblem, int maxIter, int population, double crossProb, int boolElit, int mutation, int debug, int endCriterial) {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String users = conf.get("hadoop.job.ugi");
		String[] commas = users.split(",");
		String userName = commas[0];
		
		//PARAMETROS PROBLEMA TARGET_PHRASE
		int gene_number = 0;
		String target="Hello_world!";
		String result ="";
		//Como se van a generar individuos con un numero de genes igual a la longitud
		//de la palabra objetivo, su posibilidad de mutación será su inversa...
		float mutationrate= 1.0f/(float)target.length();
		
		
		//PARAMETROS PROBLEMA ONEMAX
//		int population= 512;
//	    int gene_number = 64;
//		int maxiter = 1;
//		int boolElit = 1;
//		int mutation = 1;
//		float mutationrate= 1.0f/(float)gene_number;
//		double crossProb = 0.6;
//		int debug = 1;
//		String result ="";
//		String numProblem = "2"; //1-->'Frase Objetivo', 2-->'OneMAX', 3-->'PPeaks'
//		String target = "";
//		int endCriterial = 0; //0-->'Por iteraciones', 1-->'Por convergencia'
		
		//PARAMETROS PROBLEMA PPEAKS
//		int population= 512;
//	    int gene_number = 32;
//		int maxiter = 1;
//		int boolElit = 1;
//		int mutation = 1;
//		float mutationrate= 1.0f/(float)gene_number;
//		double crossProb = 0.6;
//		int debug = 1;
//		String result ="";
//		String numProblem = "3"; //1-->'Frase Objetivo', 2-->'OneMAX', 3-->'PPeaks'
//		String target = "";
//		int endCriterial = 0; //0-->'Por iteraciones', 1-->'Por convergencia'
		
		
		
		Coordinador coord = new Coordinador(userName);
		
		/**
		 * PASO 1.- Generamos la poblacion inicial y los ficheros de configuracion 
		 * para los nodos Worker... 
		 */
		generatePopulationFile(target,population,gene_number,Integer.parseInt(numProblem));
		generateMapperConfigurationFile(target, population, boolElit, debug,gene_number);
		generateReducerConfigurationFile(population, maxIter,boolElit,mutationrate,mutation,crossProb,target); 
		

		/**
		 * PASO 2.- El coordinador realizara las iteraciones pertinentes y devolvera el resultado buscado...
		 */
		System.out.println("CLIENTE: Lanzando trabajo...");
        final long startTime = System.currentTimeMillis();
		try {
			result = coord.readFromClientAndIterate(population, maxIter, debug, boolElit, numProblem, endCriterial,gene_number);
		} catch (IOException e) {
			System.err.println("CLIENTE: Se ha producido un error de I/O en la conexion al HDFS");
		}catch (Exception e) {
		// TODO Auto-generated catch block
		System.err.println("CLIENTE: Se ha producido un error generico ejecutando el codigo del Master");
		}
		System.out.println("CLIENTE: Job finished! "+result);
		final double duration = (System.currentTimeMillis() - startTime)/1000.0;
	    System.out.println("CLIENTE: Trabajo finalizado en " + duration + " segundos");
		System.out.println("****FIN DE EJECUCION****");
		
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 8) {
			System.err.println("Usage: mrpga <numProblem> <nIterations> <sizePop> <crossProb> <boolElit> <mutation> <debug> <endCriterial>");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		String numProblem = args[0];
		int	numIterations = Integer.parseInt(args[1]);
		int sizePop = Integer.parseInt(args[2]);
		double crossProb = Double.parseDouble(args[3]);
		int boolElit = Integer.parseInt(args[4]);
		int mutation = Integer.parseInt(args[5]);
		int debug = Integer.parseInt(args[6]);
		int endCriterial = Integer.parseInt(args[7]);
		launch(numProblem, numIterations, sizePop, crossProb, boolElit, mutation, debug, endCriterial);
		return 0;
	}
	
	public static void main(String[] argv) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Cliente(), argv);
	}

}
