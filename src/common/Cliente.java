package common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException; 


/**
 * Punto de entrada del sistema, a trav&#233;s del cual se introducir&#225;n los distintos
 * par&#225;metros configurables del mismo, tales como n&#250;mero de iteraciones, tama&#241;o
 * de poblaci&#243;n, etc.
 * @author Alberto Luengo Cabanillas
 */
public class Cliente extends Configured implements Tool {

	private static Hashtable parseXMLFile(String userName, String userDir,String problemName){
    	Hashtable configValues = new Hashtable();
    	String configLine="";
    	//Instanciamos el fichero...
		String sFile ="problem_params.dat";
		File initPop = new File(userDir+userName,sFile);
		//Miramos si existe...
		if (initPop.exists()){
			System.out.println("CLIENTE: Regenerando fichero de parametros del problema...");
			initPop.delete();
		}
		else
		{
			//Creamos el fichero....
			try {
				// A partir del objeto File creamos el fichero fisicamente
				if (initPop.createNewFile())
				{
					System.out.println("CLIENTE:El fichero de parametros del problema ha sido creado correctamente!");
					Runtime.getRuntime().exec("chmod 777 " +userDir+userName+"/problem_params.dat");
				}
				else
					System.out.println("CLIENTE:El fichero de parametros del problema no ha podido ser creado...");
			}catch (IOException e){
				e.printStackTrace();
			}
		}
    	
    	try {	
    		BufferedWriter bw= new BufferedWriter(new FileWriter(userDir+userName+"/problem_params.dat"));
			System.out.println("CLIENTE: Escribiendo en fichero de parametros del problema...");
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse (new File("config/"+problemName+".xml"));
            //Normalizamos la representación del texto...
            doc.getDocumentElement().normalize();
            NodeList listOfProperties = doc.getElementsByTagName("property");
           
            for(int s=0; s<listOfProperties.getLength() ; s++){
                Node firstPropertyNode = listOfProperties.item(s);
                if(firstPropertyNode.getNodeType() == Node.ELEMENT_NODE){
                    Element firstPropertyElement = (Element)firstPropertyNode;
                    NodeList propNameList = firstPropertyElement.getElementsByTagName("name");
                    Element propNameElement = (Element)propNameList.item(0);
                    NodeList textFNList = propNameElement.getChildNodes();
                    String propertyName = ((Node)textFNList.item(0)).getNodeValue().trim();
                
                    NodeList propValueList = firstPropertyElement.getElementsByTagName("value");
                    Element propValueElement = (Element)propValueList.item(0);
                    NodeList textLNList = propValueElement.getChildNodes();
                    String propertyValue = ((Node)textLNList.item(0)).getNodeValue().trim();
                       
                    //Añadimos el par clave-valor a una tabla Hash
                    //Escribimos el par clave-valor en un fichero local
                    configLine =propertyName+":"+propertyValue;
                    bw.write(configLine +"\n");
                    configValues.put(propertyName, propertyValue);
                }
            }
            //Cerramos el fichero
			System.out.println("CLIENTE: Cerrando fichero de parámetros de problema...");
			bw.close();
            
            //Hacemos comprobacion de valores mínimos (targetFitness y bestFitness)
            if (!configValues.containsKey("targetFitness")) {
            	System.err.println("CLIENTE: Error parseando XML!");
            	System.err.println("CLIENTE: No se ha podido encontrar el nombre de la propiedad: targetFitness");
            	System.err.println("CLIENTE: Saliendo de la aplicación...");
            	System.exit(0);
            }
            
            if (!configValues.containsKey("bestFitness")) {
            	System.err.println("CLIENTE: Error parseando XML!");
            	System.err.println("CLIENTE: No se ha podido encontrar el nombre de la propiedad: bestFitness");
            	System.err.println("CLIENTE: Saliendo de la aplicación...");
            	System.exit(0);
            }
            if (!(((String)configValues.get("bestFitness")).equals("minor")) && !(((String)configValues.get("bestFitness")).equals("major"))) {
            	System.err.println("CLIENTE: Error parseando XML!");
        		System.err.println("CLIENTE: La propiedad bestFitness debe tener un valor igual a 'minor' o 'major'");
        		System.err.println("CLIENTE: Saliendo de la aplicación...");
        		System.exit(0);
            }
        
        }catch (SAXParseException err) {
        System.err.println ("** Error parseando" + ", linea " 
             + err.getLineNumber () + ", uri " + err.getSystemId ());
        System.err.println(" " + err.getMessage ());

        }catch (SAXException e) {
        Exception x = e.getException ();
        ((x == null) ? e : x).printStackTrace ();

        }catch (Throwable t) {
        t.printStackTrace ();
        }
      return configValues;
    }
	
	/**
	 * M&#233;todo privado que genera un String compuesto de caracteres alfanum&#233;ricos, elegidos de forma aleatoria, y de longitud la del String
	 * que se le pasa por par&#225;metro.
	 * @param target Cadena que se utilizar&#225; para calcular la longitud del String a generar.
	 * @return Cadena de texto alfanum&#233;rica compuesta por caracteres generados aleatoriamente.
	 */
	private static String generateRandomString(int geneNumber, String chromKind) {
			String individuo = "";
			Random r = new Random(System.nanoTime());
			char spanish_chars[]={'!','¡','.','¿','?','_',',',';','á','é','í','ó','ú'};
			Arrays.sort(spanish_chars);
			int cont=0;
			double prob =0.0;
			if (chromKind.equals("binary"))
				while (cont < geneNumber)
				{
					prob = java.lang.Math.random();
					if ((prob <= 0.5))
						individuo += '0';
					else
						individuo += '1';
					cont ++;
				}
			else
				while ( cont < geneNumber){
					char c = (char)r.nextInt(255);
					//position = Arrays.binarySearch(spanish_chars, c);
					if((c>='0' && c<='9') || (c>='a' && c<='z') || (c>='A' && c<='Z'))
					{
						individuo += c;
						cont ++;
					}
				}
			return individuo;
			}
	
	
	/**
	 * M&#233;todo privado que genera el fichero con la poblaci&#243;n inicial de individuos a procesar por el sistema.
	 * @param target Frase objetivo a conseguir (aplicable para el problema "TargetPhrase")
	 * @param sizePop N&#250;mero entero que indica el tama&#209;o de las poblaciones a procesar
	 * @param geneNumber Longitud (entera) de los individuos que componen las poblaciones a procesar.
	 * @param numProblem N&#250;mero que indica el n&#250;mero de problema a ejecutar (1-->"TargetPhrase", 2-->"OneMAX", 3-->"PPEAKS")
	 * @param userDir Directorio personal del usuario que ejecuta la aplicacion
	 */
	private static void generatePopulationFile(String userName, int sizePop, int geneNumber, String chromKind, String userDir) {
		//Instanciamos el fichero...
		String sFile ="population.dat";
		File initPop = new File(userDir+userName,sFile);
		//Miramos si existe...
		if (initPop.exists()){
			System.out.println("CLIENTE: Regenerando fichero de poblacion...");
			initPop.delete();
		}
		else
		{
			//Creamos el fichero....
			try {
				// A partir del objeto File creamos el fichero fisicamente
				if (initPop.createNewFile())
				{
					System.out.println("CLIENTE: El fichero de poblacion ha sido creado correctamente!");
					Runtime.getRuntime().exec("chmod 777 " +userDir+userName+"/population.dat");
				}
				else
					System.out.println("CLIENTE: El fichero de poblacion no ha podido ser creado...");
			}catch (IOException e){
				e.printStackTrace();
			}
		}
		
		//Escribimos en el fichero previamente creado...
		try {
			BufferedWriter bw= new BufferedWriter(new FileWriter(userDir+userName+"/population.dat"));
			System.out.println("CLIENTE: Escribiendo en fichero de poblacion...");
			String word="";
			int i=0;
			while (i <sizePop) {
				word=generateRandomString(geneNumber, chromKind);
				//Escribimos a fichero...	
				bw.write(word +"\n");
				i++;
			}
			//Cerramos el fichero
			System.out.println("CLIENTE: Cerrando fichero de poblacion...");
			bw.close();
		} catch (IOException e){e.printStackTrace();}
	}
	
	
	private static void generateGeneralConfigurationFile(int numReducers, int numIterations,
			int numpop, int popPerMapper, int geneNumber, String chromKind, int boolElit, 
			int mutation, double mutationrate, double crossProb,int debug, 
			int tournWindow, int endCriterial, String userDir, String userName) {
	
		//Instanciamos el fichero...
		String sFile ="general_params.dat";
		File initPop = new File(userDir+userName,sFile);
		//Miramos si existe...
		if (initPop.exists()){
			System.out.println("CLIENTE: Regenerando fichero de configuracion general...");
			initPop.delete();
		}
		else
		{
			//Creamos el fichero....
			try {
				// A partir del objeto File creamos el fichero fisicamente
				if (initPop.createNewFile()){
					System.out.println("CLIENTE: El fichero de configuracion general ha sido creado correctamente!");
					Runtime.getRuntime().exec("chmod 777 " +userDir+userName+"/general_params.dat");
				}
				else
					System.out.println("CLIENTE: El fichero de configuracion general no ha podido ser creado...");
			}catch (IOException e){
				e.printStackTrace();
			}
		}
		//Escribimos en el fichero previamente creado
		try {
			BufferedWriter bw= new BufferedWriter(new FileWriter(userDir+userName+"/general_params.dat"));
			System.out.println("CLIENTE: Escribiendo en fichero de configuracion general...");
			bw.write("numReducers:"+numReducers+"\r\n");
			bw.write("numIterations:"+numIterations +"\r\n");
			bw.write("numPop:"+numpop +"\r\n");
			bw.write("popPerMapper:"+popPerMapper +"\r\n");
			bw.write("geneNumber:"+geneNumber +"\r\n");
			bw.write("chromKind:"+chromKind +"\r\n");
			bw.write("boolElit:"+boolElit +"\r\n");
			bw.write("mutation:"+mutation +"\r\n");
			bw.write("mutationRate:"+mutationrate +"\r\n");
			bw.write("crossProb:"+crossProb +"\r\n");
			bw.write("tournWin:"+tournWindow +"\r\n");
			bw.write("debug?:"+debug +"\r\n");
			bw.write("endCriterial:"+endCriterial +"\r\n");
			bw.write("userDir:"+userDir +"\r\n");
			bw.write("userName:"+userName +"\r\n");
			
			//Cerramos el fichero
			System.out.println("CLIENTE: Cerrando fichero de configuracion general...");
			bw.close();
		} catch (IOException e){e.printStackTrace();}	
	}
	
	/**
	 * M&#233;todo que inicia el sistema <code>MRPGA</code>, obteniendo la configuraci&#243;n necesaria del HDFS subyacente
	 * (par&#225;metros, sistemas de ficheros,etc).
	 * @param numProblem N&#250;mero que indica el n&#250;mero de problema a ejecutar (1-->"TargetPhrase", 2-->"OneMAX", 3-->"PPEAKS").
	 * @param numReducers @param numReducers N&#250;mero de tareas <code>Reducer</code> que lanzar&#225; el trabajo <code>MapReduce</code>
	 * @param maxIter N&#250;mero m&#225;ximo de iteraciones por las que va a atravesar el sistema.
	 * @param population Tama&#209;o (entero) de la poblaci&#243;n a procesar.
	 * @param geneNumber Longitud de los individuos de las poblaciones a procesar (no aplicable al problema <code>TargetPhrase</code>).
	 * @param crossProb Probabilidad de cruce entre dos individuos de una misma poblacion.
	 * @param boolElit N&#250;mero entero (1-->"S&#237;", 0-->"No") que indica si se introduce elitismo o no en la generaci&#243;n de descendencia.
	 * @param mutation N&#250;mero entero (1-->"S&#237;", 0-->"No") que indica si se introduce mutaci&#243;n o no en la generaci&#243;n de descendencia.
	 * @param debug N&#250;mero entero (1-->"S&#237;", 0-->"No") que indica si interesa guardar un hist&#243;rico de poblaciones procesadas en un directorio 'oldPopulations' del HDFS.
	 * @param endCriterial N&#250;mero entero (0-->"Por Iteraciones", 1-->"Por Objetivo") que indica la forma de terminaci&#243;n del algoritmo.
	 * @param userDir Directorio personal del usuario.
	 */
	void launch(String problemName, String reducerName, int numReducers, int maxIter, int population, int geneNumber, String chromKind, double crossProb, int boolElit, int mutation, double mutationRate, int tournWindow, int debug, int endCriterial, String userDir) {
		
		Hashtable configValues = new Hashtable();
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String users = conf.get("hadoop.job.ugi");
		int numMappers = Integer.parseInt(conf.get("mapred.map.tasks"));
		String[] commas = users.split(",");
		String userName = commas[0];
		String result ="";
	
		Coordinador coord = new Coordinador(userName, userDir);
		
		//Calculamos cuántos elementos maneja cada mapper, si el numero de individuos
		//de la población mod numMappers es distinto que 0, buscamos el siguiente número
		//por arriba, para una distribución equitativa...
		while(population%numMappers !=0)
			population++;
		//¡¡¡¡¡OJO!!!!CAMBIAR!!!!
		//int popPerMapper = (population / numMappers);
		int popPerMapper = population;
		
		
		/**
		 * PASO 1.- Miramos si el problema tiene parametros de configuracion especificos,
		 * parseando el XML correspondiente
		 */
		configValues = parseXMLFile(userName, userDir, problemName);
		
		/**
		 * PASO 2.- Generamos la poblacion inicial y los ficheros de configuracion 
		 * para los nodos Worker... 
		 */
		generatePopulationFile(userName,population,geneNumber,chromKind, userDir);
		
		generateGeneralConfigurationFile(numReducers, maxIter,population, popPerMapper, 
				geneNumber, chromKind, boolElit, mutation, mutationRate, crossProb, 
				debug, tournWindow, endCriterial, userDir, userName);
		
		

		/**
		 * PASO 3.- El coordinador realizara las iteraciones pertinentes y devolvera el resultado buscado...
		 */
		System.out.println("CLIENTE: Lanzando trabajo...");
        final long startTime = System.currentTimeMillis();
		try {
			result = coord.readFromClientAndIterate(population, numReducers, maxIter, debug, boolElit, problemName, reducerName, endCriterial, geneNumber, configValues, userDir);
		} catch (IOException e) {
			System.err.println("CLIENTE: Se ha producido un error de I/O en la conexion al HDFS");
		}catch (Exception e) {
			System.err.println("CLIENTE: Se ha producido un error generico ejecutando el codigo del Master");
		}
		/**
		 * PASO 3.- El cliente imprime el resultado del procesado de individuos...
		 */
		System.out.println("CLIENTE: "+ result);
		final double duration = (System.currentTimeMillis() - startTime)/1000.0;
	    System.out.println("CLIENTE: Trabajo finalizado en " + duration + " segundos");
		System.out.println("****FIN DE EJECUCION****");
		
	}

	/**
	 * M&#233;todo de la clase <code>ToolRunner</code> que parsea los par&#225;metros 
	 * introducidos por consola, ejecuta el m&#233;todo <code>launch</code> y accede 
	 * a la configuraci&#243;n (clase <code>Configuration</code>) del HDFS subyacente.
	 * @param args Array de par&#225;metros introducidos por consola.
	 * @return C&#243;digo de salida (0-->"Ejecuci&#243;n correcta", -1-->"Error").
	 */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 15) {
			System.err.println("***********************************");
			System.err.println("Uso: hadoop jar mrpga.jar <mapperName> <reducerName> <nReducers> <nIterations> <sizePop> <geneNumber> <chromKind> <crossProb> <boolElit> <mutation> <mutationRate> <tournWindow> <debug> <endCriterial> <user_dir>");
			System.err.println("***********************************");
			ToolRunner.printGenericCommandUsage(System.err);
			System.err.println("***********************************");
			return -1;
		}
		String problemName = args[0];
		System.out.println("MAPPER CLASS NAME: "+problemName);
		String reducerName = args[1];
		System.out.println("REDUCER CLASS NAME: "+reducerName);
		int	numReducers = Integer.parseInt(args[2]);
		System.out.println("NUM REDUCERS: "+numReducers);
		int	numIterations = Integer.parseInt(args[3]);
		System.out.println("NUM ITERATIONS: "+numIterations);
		int sizePop = Integer.parseInt(args[4]);
		System.out.println("POPULATION: "+sizePop);
		int geneNumber = Integer.parseInt(args[5]);
		System.out.println("GENE NUMBER: "+geneNumber);
		String chromKind = args[6];
		System.out.println("CHROM KIND: "+chromKind);
		double crossProb = Double.parseDouble(args[7]);
		System.out.println("CROSSPROB: "+crossProb);
		int boolElit = Integer.parseInt(args[8]);
		System.out.println("ELITISM?: "+boolElit);
		int mutation = Integer.parseInt(args[9]);
		System.out.println("MUTATION?: "+mutation);
		double mutationRate = Double.parseDouble(args[10]);
		if (mutation !=0)
			System.out.println("MUTATION RATE: "+mutationRate);
		int tournWindow = Integer.parseInt(args[11]);
		System.out.println("TOURN.WINDOW?: "+tournWindow);
		int debug = Integer.parseInt(args[12]);
		System.out.println("DEBUG?: "+debug);
		int endCriterial = Integer.parseInt(args[13]);
		System.out.println("END CRITERIAL: "+endCriterial);
		String userDir = args[14];
		System.out.println("USER DIR: "+userDir);
		launch(problemName, reducerName, numReducers, numIterations, sizePop, geneNumber, chromKind, crossProb, boolElit, mutation, mutationRate, tournWindow, debug, endCriterial, userDir);
		return 0;
	}
	/**
	 * M&#233;todo principal de la clase <code>Cliente</code> cuya &#250;nica funci&#243;n es 
	 * llamar al m&#233;todo <code>run</code> de la misma clase.
	 * 
	 * @param argv Array de comandos introducidos por consola en tiempo de ejecucion.
	 * @throws Exception Excepci&#243;n gen&#233;rica.
	 */
	public static void main(String[] argv) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Cliente(), argv);
	}

}
