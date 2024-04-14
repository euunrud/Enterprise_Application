import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class IMDBStudent20200974 {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: IMDBStudent <input> <output>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("IMDBStudent")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> genres = lines.map(line -> line.split("::")[2]);

        JavaRDD<String> allGenres = genres.flatMap(genre -> Arrays.asList(genre.split("\\|")).iterator()); 

        JavaPairRDD<String, Integer> genreCnts = allGenres.mapToPair(genre -> new Tuple1<>(genre, 1));

        JavaPairRDD<String, Integer> genreWordCnts = genreCnts.reduceByKey((a, b) -> a + b);

        genreWordCnts.saveAsTextFile(args[1]);

        spark.stop();
    }
}
