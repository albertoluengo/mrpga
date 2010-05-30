package common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Random;

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

public class Cliente {

	
	private static String generateRandomString(String target) {
			String individuo = "";
			int sizeTarget=target.length();
			Random r = new Random(System.nanoTime());
			char spanish_chars[]={'!','¡','.','¿','?','_',',',';','á','é','í','ó','ú'};
			Arrays.sort(spanish_chars);
			int i=0;
			int position = 0;
				while ( i < sizeTarget){
					char c = (char)r.nextInt(255);
					Arrays.sort(spanish_chars);
					position = Arrays.binarySearch(spanish_chars, c);
					if((c>='0' && c<='9') || (c>='a' && c<='z') || (c>='A' && c<='Z') || (position>=0))
					{
						individuo += c;
						i ++;
					}
				}
			return individuo;
			}
		
	private static String generateRandomBinaryString(String target) {
		String cadenaAleatoria ="";
		int sizeTarget=target.length();
		//long milis = new java.util.GregorianCalendar().getTimeInMillis();
		//Random r = new Random(milis);
		Random r = new Random(System.nanoTime());
		int d = 2; // nº de decimales
		int u = 10^d; // Hay que usar la función para potencias.
		float i = 1 + r.nextFloat() % u;
		float q = i/u; //devuelve entre 0 y 1.
		int cont=0;
			while ( cont < sizeTarget){
				if ((q < 0.5))
					cadenaAleatoria += '0';
				else
					cadenaAleatoria += '1';
				i ++;
			}
		return cadenaAleatoria;
		}
	
	
	private static void generatePopulationFile(String target, int sizePop, int numProblem) {
		
		//Instanciamos el fichero...
		String sFile ="population.txt";
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
		//Escribimos en el fichero previamente creado
		try {
			BufferedWriter bw= new BufferedWriter(new FileWriter(sFile));
			System.out.println("CLIENTE: Escribiendo en fichero de poblacion...");
			String word="";
			int i=0;
			while (i <sizePop) {
				if (numProblem == 1) //Caso del problema de la 'frase objetivo'
					//Generamos la palabra descendiente
					word=generateRandomString(target);
				else
					word=generateRandomBinaryString(target);
				//Escribimos a fichero...	
				bw.write(word +"\r\n");
				i++;
			}
			//Cerramos el fichero
			System.out.println("CLIENTE: Cerrando fichero de poblacion...");
			bw.close();
		} catch (IOException e){e.printStackTrace();}
	}
	
	//Creamos el fichero de configuracion que debe subir el coordinador al HDFS para que el
	//Master lo distribuya entre los nodos trabajadores...
	private static void generateMapperConfigurationFile(String target, int numPop, int boolElit, int debug) {
		
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
			bw.write(debug +"\n");
			bw.write(boolElit +"\r\n");
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
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		/*int population = 0;
		int maxiter = 0;
		float elitrate = 0;
		float mutationrate = 0;
		double rand = Math.random();
		float mutation = 0;
		String target ="";*/
		
		int population= 2050;
		int maxiter = 3;
		int boolElit = 1;
		String target="Hello_world!";
		//Como se van a generar individuos con un numero de genes igual a la longitud
		//de la palabra objetivo, su posibilidad de mutación será su inversa...
		int mutation = 1;
		float mutationrate= 1.0f/(float)target.length();
		double crossProb = 0.6;
		int debug = 1;
		String result ="";
		String numProblem = "1"; //1-->'Frase Objetivo', 2-->'OneMAX', 3-->'PPeaks'
		int endCriterial = 0; //0-->'Por iteraciones', 1-->'Por convergencia'
		
		Coordinador coord = new Coordinador();
	
		BufferedReader dataIn = new BufferedReader(new InputStreamReader(System.in));
		
		System.out.println("*****MENU PRINCIPAL*****");
		
		/*System.out.print("Introduzca el tamaño de la poblacion: ");
		try {
			population=Integer.parseInt(dataIn.readLine());
		}catch (IOException e){
		System.err.println("Error fetching the population!");	
		}
		
		System.out.print("Introduzca el numero maximo de iteraciones: ");
		try {
			maxiter=Integer.parseInt(dataIn.readLine());
		}catch (IOException e){
		System.err.println("Error fetching the iterations!");	
		}
		
		System.out.print("Desea elitismo en la descendencia?: ");
		try {
			boolElit = Integer.parseInt(dataIn.readLine());
		}catch (IOException e){
		System.err.println("Error fetching the elitism rate!");	
		}
		
		System.out.print("Desea introducir mutacion en la descendencia?: ");
		try {
			mutation=Integer.parseInt(dataIn.readLine());
		}catch (IOException e){
		System.err.println("Error fetching the mutation boolean!");	
		}*/
		
		/*
		if (mutation==1)
		{
			System.out.print("Que grado de mutacion desea introducir?: ");
			try {
				mutationRate=Float.parseFloat(dataIn.readLine());
			}catch (IOException e){
			System.err.println("Error fetching the mutation rate!");	
			}
		}
		*/
		
		/*System.out.print("Introduzca la probabilidad de cruce: ");
		try {
			crossProb=dataIn.readLine();
		}catch (IOException e){
		System.err.println("Error fetching the cross probability!");	
		}*/
		
		
		/*System.out.print("Introduzca la frase objetivo a conseguir: ");
		try {
			target=dataIn.readLine();
		}catch (IOException e){
		System.err.println("Error fetching the target phrase!");	
		}*/
		
		/*System.out.print("¿Desea activar la opcion de debug?: ");
		try {
			debug=dataIn.readLine();
		}catch (IOException e){
		System.err.println("Error fetching the debug option!");	
		}*/
		
		/**PASO 1.- Generamos la poblacion inicial y los ficheros de configuracion 
		 * para los nodos Worker... 
		 */
		generatePopulationFile(target,population,Integer.parseInt(numProblem));
		generateMapperConfigurationFile(target, population, boolElit, debug);
		generateReducerConfigurationFile(population, maxiter,boolElit,mutationrate,mutation,crossProb,target); 
		
		
		/*Process theProcess = null;
		BufferedReader inStream = null;
		System.out.println("llamando a la clase Coordinador");
		
		try{
			theProcess = Runtime.getRuntime().exec("java Coordinador");
		}
		catch (IOException e)
		{
			System.err.println("Error en exec() method");
			e.printStackTrace();
		}
		
		try{
			inStream = new BufferedReader(new InputStreamReader(theProcess.getInputStream()));
			System.out.println(inStream.readLine());
		}
		catch (IOException e)
		{
			System.err.println("Error on inStream.readLine()");
			e.printStackTrace();
		}*/
		
		//PASO 2.- El coordinador realizara las iteraciones pertinentes y devolvera el resultado buscado...
		System.out.println("CLIENTE: Lanzando trabajo...");
        final long startTime = System.currentTimeMillis();
		try {
			result = coord.readFromClientAndIterate(population, maxiter, debug, boolElit, numProblem, endCriterial);
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

}
