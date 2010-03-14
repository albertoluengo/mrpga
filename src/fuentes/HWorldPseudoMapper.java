package fuentes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Hashtable;
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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
//import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HWorldPseudoMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text subjectAsWord = new Text();
	private String mapTaskId = "";
	Configuration conf;
	private Hashtable mapParameters = new Hashtable();
	private int numElemProcessed = 0;
	private static final Log LOG = LogFactory.getLog(HWorldPseudoMapper.class.getName());
	final String HDFS_MAPPER_CONFIGURATION_FILE="/user/hadoop-user/data/mapper_configuration.dat";
	
	
	private IntWritable calculateFitness(String target, Text individual) {		
		int targetSize=target.length();
		String textAsString = individual.toString();
		int fitness=0;
		for (int j=0; j<targetSize; j++) {
			fitness += Math.abs((textAsString.charAt(j) - target.charAt(j)));
		}
		return new IntWritable(fitness);
	}
	
	@Override
	protected void setup(Context cont)throws IOException {
		//LOG.info("***********DENTRO DEL SETUP DEL MAPPER**********");
		conf = cont.getConfiguration();
		mapTaskId = cont.getConfiguration().get("mapred.task.id");
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
		String[]keys = {"targetPhrase","numPopulation","debugging","elitism"};
		int index=0;
		 while ((strLine = br.readLine()) != null)   {
			mapParameters.put(keys[index], strLine);
	        index++;
	      }
		 dis.close();
		 
	}
	
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString();
		int numPop = Integer.parseInt((String)mapParameters.get("numPopulation"));
		String targetPhrase = (String)mapParameters.get("targetPhrase");
		int boolElit = Integer.parseInt((String)mapParameters.get("elitism"));
		int debug = Integer.parseInt((String)mapParameters.get("debugging"));
		StringTokenizer itr = new StringTokenizer(line);
		int bestFitness = 999999; 
		Text bestIndiv = new Text();
		
		while(itr.hasMoreTokens()) {
			subjectAsWord.set(itr.nextToken());
			IntWritable elemFitness = calculateFitness(targetPhrase, subjectAsWord);
			
			//Seguimos la pista del mejor elemento...
			if (elemFitness.get() < bestFitness) {
				bestFitness = elemFitness.get();
				bestIndiv = subjectAsWord;
				}
			context.write(subjectAsWord, elemFitness);
			numElemProcessed++;
			
			if ((numElemProcessed == numPop -1)&&(boolElit==1)) {
				closeAndWrite(debug,bestIndiv,bestFitness);
			}
		}
	}
	
	/**Una vez todos los elementos hayan sido procesados, escribimos en un
	 * fichero global el mejor de ellos (si queremos introducir elitismo)...
	 */
	public void closeAndWrite(int debug,Text bestIndiv, int bestFitness) throws IOException {
		String bestFile = "/user/hadoop-user/bestIndividuals/bestIndiv.txt";
		String bestDir = "/user/hadoop-user/bestIndividuals";
		Path bestIndivPath = new Path(bestFile);
		Path bestDirPath = new Path(bestDir);
		FileSystem hdfs = FileSystem.get(new Configuration());
		if (hdfs.exists(bestDirPath)) {
    		//Eliminamos el directorio de los mejores individuos primero...
    		hdfs.delete(bestDirPath,true);
    	}
		FSDataOutputStream dos = hdfs.create(bestIndivPath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(dos));
		bw.write(bestIndiv.toString());
		bw.close();
	}	
	
}
