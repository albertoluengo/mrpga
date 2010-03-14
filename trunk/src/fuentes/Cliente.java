package fuentes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
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
			String cadenaAleatoria = "";
			int sizeTarget=target.length();
			//long milis = new java.util.GregorianCalendar().getTimeInMillis();
			//Random r = new Random(milis);
			Random r = new Random(System.nanoTime());
	
			int i=0;
				while ( i < sizeTarget){
				char c = (char)r.nextInt(1000);
					if ((c >= 'a' && c <='z') || (c >='A' && c <='Z'))
					{
						cadenaAleatoria += c;
						i ++;
					}
				}
			return cadenaAleatoria;
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
			System.out.println("Regenerando fichero de poblacion...");
			initPop.delete();
		}
		else
		{
			//Creamos el fichero....
			try {
				// A partir del objeto File creamos el fichero fisicamente
				if (initPop.createNewFile())
					System.out.println("El fichero de poblacion ha sido creado correctamente!");
				else
					System.out.println("El fichero de poblacion no ha podido ser creado...");
			}catch (IOException e){
				e.printStackTrace();
			}
		}
		//Escribimos en el fichero previamente creado
		try {
			BufferedWriter bw= new BufferedWriter(new FileWriter(sFile));
			System.out.println("Escribiendo en fichero de poblacion...");
			String word="";
			int i=0;
			while (i <sizePop) {
				if (numProblem == 1) //Caso del problema del 'Hola Mundo'
					//Generamos la palabra descendiente
					word=generateRandomString(target);
				else
					word=generateRandomBinaryString(target);
				//Escribimos a fichero...	
				bw.write(word +"\r\n");
				i++;
//				try {
//					Thread.sleep(15);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
			//Cerramos el fichero
			System.out.println("Cerrando fichero de poblacion...");
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
			System.out.println("Regenerando fichero de configuracion para Mappers...");
			initPop.delete();
		}
		else
		{
			//Creamos el fichero....
			try {
				// A partir del objeto File creamos el fichero fisicamente
				if (initPop.createNewFile())
					System.out.println("El fichero de configuracion para Mappers ha sido creado correctamente!");
				else
					System.out.println("El fichero de configuracion para Mappers no ha podido ser creado...");
			}catch (IOException e){
				e.printStackTrace();
			}
		}
		//Escribimos en el fichero previamente creado
		try {
			BufferedWriter bw= new BufferedWriter(new FileWriter(sFile));
			System.out.println("Escribiendo en fichero de configuracion para Mappers...");
			bw.write(target +"\r\n");
			bw.write(numPop +"\r\n");
			bw.write(debug +"\n");
			bw.write(boolElit +"\r\n");
			//Cerramos el fichero
			System.out.println("Cerrando fichero de configuracion para Mappers...");
			bw.close();
		} catch (IOException e){e.printStackTrace();}	
	}
	
	//Creamos el fichero de configuracion que debe subir el coordinador al HDFS para que el
	//Master lo distribuya entre los nodos trabajadores...
	private static void generateReducerConfigurationFile(int numpop, int maxiter, int boolElit,
			float mutationrate, float mutation, String target) {
		
		//Instanciamos el fichero...
		String sFile ="reducer_configuration.dat";
		File initPop = new File("./",sFile);
		//Miramos si existe...
		if (initPop.exists()){
			System.out.println("Regenerando fichero de configuracion para Reducers...");
			initPop.delete();
		}
		else
		{
			//Creamos el fichero....
			try {
				// A partir del objeto File creamos el fichero fisicamente
				if (initPop.createNewFile())
					System.out.println("El fichero de configuracion para Reducers ha sido creado correctamente!");
				else
					System.out.println("El fichero de configuracion para Reducers no ha podido ser creado...");
			}catch (IOException e){
				e.printStackTrace();
			}
		}
		//Escribimos en el fichero previamente creado
		try {
			BufferedWriter bw= new BufferedWriter(new FileWriter(sFile));
			System.out.println("Escribiendo en fichero de configuracion para Reducers...");
			bw.write(numpop +"\r\n");
			bw.write(maxiter +"\r\n");
			bw.write(boolElit +"\r\n");
			bw.write(mutationrate +"\r\n");
			bw.write(mutation +"\r\n");
			bw.write(target);
			//Cerramos el fichero
			System.out.println("Cerrando fichero de configuracion para Reducers...");
			bw.close();
		} catch (IOException e){e.printStackTrace();}	
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		/*int population = 0;
		int maxiter = 0;
		float elitrate = 0;
		float mutationrate = 0;
		double rand = Math.random();
		float mutation = 0;
		String target ="";*/
		
		int population=1000;
		int maxiter =3;
		int boolElit = 1;
		float mutationrate=0.25f;
		String target="Hello world!";
		double rand = Math.random();
		float mutation = 0;
		int debug = 1;
		String result ="";
		String numProblem = "1"; //1-->'Frase Objetivo', 2-->'OneMAX', 3-->'PPeaks'
		
		Coordinador coord = new Coordinador();
	
		BufferedReader dataIn = new BufferedReader(new InputStreamReader(System.in));
		
		System.out.println("*****MENU PRINCIPAL*****");
		
		/*System.out.print("Introduzca el tamanho de la poblacion: ");
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
		
		System.out.print("Introduzca el grado de mutacion en la descendencia: ");
		try {
			mutationrate=Float.parseFloat(dataIn.readLine());
		}catch (IOException e){
		System.err.println("Error fetching the mutation rate!");	
		}*/
		
		mutation = (float)(rand * mutationrate);
		
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
		generateReducerConfigurationFile(population, maxiter,boolElit,mutationrate,mutation,target); 
		
		
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
		try {
			result = coord.readFromClientAndIterate(population, maxiter, debug, boolElit, numProblem);
		} catch (IOException e) {
			System.err.println("CLIENTE: Se ha producido un error de I/O en la conexion al HDFS");
		}catch (Exception e) {
		// TODO Auto-generated catch block
		System.err.println("CLIENTE: Se ha producido un error ejecutando el codigo del Master");
		}
		System.out.println("CLIENTE: El resultado que obtenemos es: "+result);
		System.out.println("****FIN DE EJECUCION****");
		
	}

}
