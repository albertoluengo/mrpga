package fuentes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.StringTokenizer;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HWorldPseudoMapper extends Mapper<Object, Text, Text, IntWritable> {
	//Palabra que representa al descendiente...
	private Text subjectAsWord = new Text();
	private int boolElit = 1;
	private int debug = 1;
	final String BEST_INDIVIDUAL_FILE = "/user/hadoop-user/bestIndividuals/bestIndiv.txt";
	Path bestIndivPath = new Path(BEST_INDIVIDUAL_FILE);
	private static final Log LOG = 
	    LogFactory.getLog(HWorldPseudoMapper.class.getName());
	
	//Parametro de configuracion que necesita el Mapper (en este caso, la palabra objetivo)
	String targetPhrase = " ";
	
	//Indicamos el fichero de configuracion que debera leer
	final String HDFS_MAPPER_CONFIGURATION_FILE="/user/hadoop-user/data/mapper_configuration.dat";
	
	private IntWritable calculateFitness(String target, Text original) {		
		int targetSize=target.length();
		String textAsString = original.toString();
		int fitness=0;
		for (int j=0; j<targetSize; j++) {
			fitness += Math.abs((textAsString.charAt(j) - target.charAt(j)));
		}
		return new IntWritable(fitness);
	}
	
	@Override
	protected void setup(Context cont)throws IOException {
		LOG.info("***********DENTRO DEL SETUP DEL MAPPER**********");
		FileSystem hdfs = FileSystem.get(new Configuration()); 
		Path path = new Path(HDFS_MAPPER_CONFIGURATION_FILE);
		//Validamos primero el path de entrada antes de leer del fichero
		if (!hdfs.exists(path))
		{
			throw new IOException("El fichero especificado " +HDFS_MAPPER_CONFIGURATION_FILE + "no existe");
		}
		
		if (!hdfs.isFile(path))
		{
			throw new IOException("El fichero especificado "+HDFS_MAPPER_CONFIGURATION_FILE + "no existe");
		}
		
		FSDataInputStream dis = hdfs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(dis));
		String strLine;
		 while ((strLine = br.readLine()) != null)   {
			targetPhrase = strLine;
	        LOG.info("LA LINEA DEL FICHERO ES "+strLine);
	      }
		 dis.close();
	}
	
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line);
		int bestFitness = 999999;
		FileSystem hdfs = FileSystem.get(new Configuration()); 
		Text bestIndiv = new Text();
		//LOG.info("***********DENTRO DEL MAPPER**********");
		while(itr.hasMoreTokens()) {
			subjectAsWord.set(itr.nextToken());
			IntWritable elemFitness = calculateFitness(targetPhrase, subjectAsWord);
			context.write(subjectAsWord, elemFitness);
			
			//Buscamos el mejor elemento
			if (elemFitness.get() < bestFitness) {
				bestFitness = elemFitness.get();
				bestIndiv = subjectAsWord;
				}
		}
		/**Una vez todos los elementos hayan sido procesados, escribimos en un
		 * fichero global el mejor de ellos (si queremos introducir elitismo)...
		 */
		if (boolElit ==1) {
			FSDataOutputStream dos = hdfs.create(bestIndivPath, true);
			/**Si el valor para debug que leemos del fichero es true, creamos un fichero
			 * nuevo cada iteración, si no lo sobreescribimos (overwrite=true)
			 */
			if (debug==0) {
				hdfs.delete(bestIndivPath,true);
				dos = hdfs.create(bestIndivPath, true);
			}
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(dos));
			bw.write(bestIndiv.toString());
			bw.close();
		}
	}
	
	
//	public void map(Text key, Text value,
//            OutputCollector<Text, IntWritable> output, 
//            Reporter reporter) throws IOException, InterruptedException 
//	{
//		String line = value.toString();
//		//System.out.println("MAPPER: La linea que estoy leyendo es: "+line.toString());
//		StringTokenizer itr = new StringTokenizer(line);
//		reporter.setStatus("The target word is " + targetPhrase);
//		while(itr.hasMoreTokens()) {
//			subjectAsWord.set(itr.nextToken());
//			fitness=calculateFitness(targetPhrase);
//			output.collect(subjectAsWord, new IntWritable(fitness));
//			//context.write(subjectAsWord, new IntWritable(fitness));
//		}
//	}
	
	
	
}
