import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.util.Arrays;
import java.util.Iterator;

public class IMDBStudent {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: IMDBStudent <input> <output>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
            .appName("IMDBStudent")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> genres = lines.map(line -> line.split("::")[2]); //map은 RDD transformation에 해당하며 함수를 적용한 결과 RDD를 되돌려 준다.

        JavaRDD<String> allGenres = genres.flatMap(genre -> Arrays.asList(genre.split("\\|")).iterator());

        JavaPairRDD<String, Integer> genreCnts = allGenres.mapToPair(genre -> new Tuple2<>(genre, 1)); //기본 RDD -> pairRDD로 변환.

        JavaPairRDD<String, Integer> genreWordCnts = genreCnts.reduceByKey((a, b) -> a + b);  //reduceByKey는 동일한 Key에 대한 value의 값을 합칠 수 있는 transformation.

        genreWordCnts.map(tuple -> "(" + tuple._1() + "," + tuple._2() + ")")
            .saveAsTextFile(args[1]);

        spark.stop();
    }
}
