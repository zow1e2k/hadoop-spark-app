import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class World {
    private ArrayList<Continent> continents;
    private Dataset<Row> data;

    public World(Dataset<Row> data) {
        this.continents = new ArrayList<>();
        this.data = data;

        return;
    }

    public void addContinent(Continent continent) {
        this.continents.add(continent);
        return;
    }

    public void removeContinent(Continent continent) {
        if (!this.continents.contains(continent)) {
            return;
        }

        this.continents.remove(continent);
        return;
    }

    public void createContinentByName(String name) {
        Continent continent = new Continent(this.data, name);
        continents.add(continent);
        return;
    }

    public ArrayList<Continent> getContinents() {
        ArrayList<Continent> result = new ArrayList<>();

        result.addAll(this.continents);
        return result;
    }
}
