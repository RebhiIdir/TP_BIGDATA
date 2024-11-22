import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TemperatureDriver {

    public static void main(String[] args) throws Exception {
        // Création d'une nouvelle configuration Hadoop
        Configuration conf = new Configuration(); //faut toujours le faire
        
        // Chemin d'entrée dans HDFS où se trouvent les fichiers de températures
        Path inputPath = new Path("hdfs://quickstart.cloudera:8020/Dtp/Temperatures.txt");
        
        // Chemin de sortie dans HDFS où les résultats du job seront écrits
        Path outputPath = new Path("hdfs://quickstart.cloudera:8020/output_temperature");

        // Créer le job MapReduce
        Job job = Job.getInstance(conf, "TemperatureDriver");
        
        // Définir la classe principale du job
        job.setJarByClass(TemperatureDriver.class);

        // Définir les classes Mapper et Reducer
        job.setMapperClass(TemperatureMapper.class);   // Mapper qui va analyser les températures
        job.setReducerClass(TemperatureReducer.class); // Reducer qui va effectuer le calcul des résultats

        // Définir les types des clés et valeurs
        job.setOutputKeyClass(Text.class);   // La clé de sortie : Nom de la ville
        job.setOutputValueClass(IntWritable.class); // La valeur de sortie : Température maximale ou minimale

        // Définir le format d'entrée (HDFS)
        FileInputFormat.setInputPaths(job, inputPath);

        // Définir le format de sortie (HDFS)
        FileOutputFormat.setOutputPath(job, outputPath);

        // Connexion à HDFS pour vérifier si le répertoire de sortie existe déjà
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://quickstart.cloudera:8020"), conf);
        
        // Si le répertoire de sortie existe déjà, on le supprime avant d'exécuter le job
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        // Lancer le job MapReduce
        boolean jobSuccess = job.waitForCompletion(true);

        // Vérifier si le job a été exécuté avec succès
        if (jobSuccess) {
            System.out.println("Le job a été exécuté avec succès !");
        } else {
            System.err.println("Le job a échoué.");
        }
    }
}
