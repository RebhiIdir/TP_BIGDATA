import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reçoit en entrée des données Text et IntWritable (villes et températures)
//Produit en sortie des dnnées Text,Text (pays, ville)
public class TemperatureReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Text result = new Text(); // Texte pour stocker les résultats filtrés

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxTemp = Integer.MIN_VALUE; // Initialisation de la température maximale
        int minTemp = Integer.MAX_VALUE; // Initialisation de la température minimale

        // Parcourir toutes les valeurs pour cette clé (ville)
        for (IntWritable val : values) {
            int temp = val.get(); // Obtenir la valeur en tant qu'entier
            maxTemp = Math.max(maxTemp, temp); // Mettre à jour la température maximale
            minTemp = Math.min(minTemp, temp); // Mettre à jour la température minimale
        }

        // Vérifier les conditions avant d'écrire les résultats
        if (maxTemp > 45 && minTemp < 0) {
            String output = "Max Temp: " + maxTemp + ", Min Temp: " + minTemp;
            result.set(output); // Créer une chaîne texte pour la sortie
            context.write(key, result); // Écrire la ville et les températures dans le contexte
            //On peut aussi utiliser l'objet Collector pour la sortie
        }
    }
}
