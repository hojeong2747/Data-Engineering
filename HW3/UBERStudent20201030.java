import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
public final class UBERStudent20201030 {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: UBERStudent20201030 <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("UBERStudent20201030")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

	JavaPairRDD<String, String> words = lines.mapToPair(new PairFunction<String, String, String>() {
		public Tuple2<String, String> call(String s) {

			String[] days = {"MON", "TUE", "WED", "THR", "FRI", "SAT", "SUN"};

                	StringTokenizer itr = new StringTokenizer(s, ",");
                	String region = itr.nextToken();
                	String tmpDate = itr.nextToken();
                	String vehicles = itr.nextToken();
                	String trips = itr.nextToken();

                	itr = new StringTokenizer(tmpDate, "/");
                	int month = Integer.parseInt(itr.nextToken());
                	int day = Integer.parseInt(itr.nextToken());
                	int year = Integer.parseInt(itr.nextToken());

                	LocalDate date = LocalDate.of(year, month, day);
               	 	DayOfWeek tmp = date.getDayOfWeek();
                	int dayNum = tmp.getValue();
                	String dayOfWeek = days[dayNum - 1];

                	String key = region + "," + dayOfWeek;
                	String value = trips + "," + vehicles;

			return new Tuple2(key, value);
		}
	});
        
        JavaPairRDD<String, String> counts = words.reduceByKey(new Function2<String, String, String>() {
        	public String call(String x, String y) {
        		String[] x_li = x.split(",");
        		String[] y_li = y.split(",");
        		
        		int trips = Integer.parseInt(x_li[0]) + Integer.parseInt(y_li[0]);
        		int vehicles = Integer.parseInt(x_li[1]) + Integer.parseInt(y_li[1]);
        		return trips + "," + vehicles;
        	}
        });
        
        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}