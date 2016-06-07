package fourth;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static fourth.ColumnHelper.getIp;
import static fourth.ColumnHelper.getPageIp;

public final class Main {

    public static void main(String[] args) throws Exception {

        if (args.length != 3)
            throw new IllegalArgumentException("Input parameters has to contain input, output and master node info");

        String input = args[0];
        String output = args[1];
        String master = args[2];

        SparkConf sparkConf = new SparkConf().setAppName("Logs").setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile(input);

        final List<String> mostActiveUsers = getMostActiveUserIps(lines);

        List<String> result = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                try {
                    return ColumnHelper.isUserPage(line);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return false;
            }
        })
                .mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String string) throws Exception {
                        try {
                            return new Tuple2<>(getPageIp(string), getIp(string));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return new Tuple2<>("", "");
                    }
                })
                .reduceByKey(new Function2<String, String, String>() {
                    @Override
                    public String call(String ip1, String ip2) throws Exception {
                        return ip1 + " " + ip2;
                    }
                })
                .map(new Function<Tuple2<String, String>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                        String[] ips = getIps(tuple);
                        return new Tuple2(ips.length, tuple._1);
                    }


                })
                .sortBy(new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple) throws Exception {
                        return String.valueOf(tuple._1);
                    }
                }, false, 1)
                .map(new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple) throws Exception {
                        return "User with id " + String.valueOf(tuple._2) + " has " + String.valueOf(tuple._1) + " hits from users";
                    }
                })
                .collect();

        Files.write(Paths.get(output), result, StandardCharsets.UTF_8);

        sc.stop();
    }

    private static List<String> getMostActiveUserIps(JavaRDD<String> lines) {
        return lines
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String string) throws Exception {
                        try {
                            return new Tuple2<>(getIp(string), 1);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return new Tuple2<>("", 0);
                    }
                })
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                })
                .map(new Function<Tuple2<String, Integer>, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple.swap();
                    }
                })
                .sortBy(new Function<Tuple2<Integer, String>, Integer>() {
                    @Override
                    public Integer call(Tuple2<Integer, String> tuple) throws Exception {
                        return tuple._1;
                    }
                }, false, 5)
                .map(new Function<Tuple2<Integer, String>, String>() {
                    @Override
                    public String call(Tuple2<Integer, String> tuple) throws Exception {
                        return tuple._2;
                    }
                })
                .take(100);
    }

    private static String[] getIps(Tuple2<String, String> tuple) {
        return ColumnHelper.SPACE.split(tuple._2);
    }


}