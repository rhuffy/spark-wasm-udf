import io.github.kawamuray.wasmtime.Func;
import io.github.kawamuray.wasmtime.Instance;
import io.github.kawamuray.wasmtime.Module;
import io.github.kawamuray.wasmtime.Store;
import io.github.kawamuray.wasmtime.WasmFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static io.github.kawamuray.wasmtime.WasmValType.I64;
import static java.util.Collections.emptyList;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;


public class Entrypoint {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://amdci2.csail.mit.edu:7077")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("people.json");
        df.createOrReplaceTempView("PEOPLE");
        df.show();
        df.printSchema();

        UserDefinedFunction myUdf = udf(new UDF2<Long, Long, Long>() {
            @Override
            public Long call(Long a, Long b) throws Exception {
                Store<Void> store = Store.withoutData();
                Module module = Module.fromFile(store.engine(), "udf.wat");
                Instance instance = new Instance(store, module, emptyList());
                Func fun = instance.getFunc(store, "add").get();
                WasmFunctions.Function2<Long, Long, Long> add = WasmFunctions.func(
                        store, fun, I64, I64, I64);

                return add.call(a, b);
            }
        }, DataTypes.LongType);

        df.select(col("name"), myUdf.apply(col("age"), col("height")).as("SUM")).show();

        spark.stop();
    }
}
