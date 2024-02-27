import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VanillaWordCount {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Need to include filename as argument");
            System.exit(1);
        }

        String fileName = args[0]; // The first command line argument
        
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            Map<Integer, Integer> maxDeath = new HashMap<>();
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(",");
                int deaths = Integer.parseInt(columns[8].replace("\"", ""));
                int year = Integer.parseInt(columns[0].split("-")[0].replace("\"", ""));
                // System.out.println(Integer.toString(year) + " had " + Integer.toString(deaths) + " deaths");
                maxDeath.merge(year, deaths, Math::max); // find the max of each year
            }

            // display results
            maxDeath.forEach((year, totalDeaths) -> {
              System.out.println(year + " had " + totalDeaths + " deaths");
            });
        } catch (IOException e) {
            System.err.println("An error occurred while reading the file: " + e.getMessage());
        }
    }
}
