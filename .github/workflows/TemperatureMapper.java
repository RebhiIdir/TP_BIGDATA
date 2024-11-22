import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text city = new Text(); // Clé : Ville, Pays
    private IntWritable maxTempWritable = new IntWritable(); // Valeur : Température maximale
    private IntWritable minTempWritable = new IntWritable(); // Valeur : Température minimale

    /*
     Méthode map qui est appelée pour chaque ligne du fichier d'entrée
     parametre value : La ligne du fichier à traiter
     parametre context : Le contexte Hadoop pour écrire la sortie
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Diviser la ligne en champs séparés par un point-virgule
        String[] fields = value.toString().split(";"); // un token est reconnu après chaque rencontre du caractère ";"
        // String tokenize() peut également faire la meme chose, c'est à dire la reconaissance des tokens
        
        // Vérifier que la ligne contient suffisamment de données
        if (fields.length >= 5) {
            try {
                // Extraire les informations : ville, pays, température maximale, température minimale
                String cityInfo = fields[1] + ", " + fields[2]; // Exemple: "Tizi-Ouzou, Algérie"
                int maxTemp = Integer.parseInt(fields[3].trim()); // Température maximale
                int minTemp = Integer.parseInt(fields[4].trim()); // Température minimale
                // trim() supprime les espaces et tabulations
                
                // Assigner la ville aux variables de sortie
                city.set(cityInfo);

                // Assigner les températures maximales et minimales
                maxTempWritable.set(maxTemp);
                minTempWritable.set(minTemp);

                // Écrire les paires clé-valeur pour chaque température (max et min)
                context.write(city, maxTempWritable); // Ville -> Température maximale
                context.write(city, minTempWritable); // Ville -> Température minimale
            } catch (NumberFormatException e) {
                // En cas d'erreur de conversion, on ignore la ligne ou on log l'erreur
            }
        }
    }
}
