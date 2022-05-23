import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.udf;

import java.util.Random;

public class UdfFactory {
    public static UserDefinedFunction createMapFunction(String functionName, String[] inputColumnNames, String outputColumnType){
        DataType outputDataType = DataType.fromDDL(outputColumnType);
        switch(inputColumnNames.length) {
            case 0:
                return udf(
                        () -> {
                            return FunctionWrapper.get().apply()[0];
                        },
                        outputDataType);
            case 1:
                return udf(
                        (a1) -> {
                            return FunctionWrapper.get().apply(a1)[0];
                        },
                        outputDataType);
            case 2:
                return udf(
                        (a1, a2) -> {
                            return FunctionWrapper.get().apply(a1, a2)[0];
                        },
                        outputDataType);
            case 3:
                return udf(
                        (a1, a2, a3) -> {
                            return FunctionWrapper.get().apply(a1, a2, a3)[0];
                        },
                        outputDataType);
            case 4:
                return udf(
                        (a1, a2, a3, a4) -> {
                            return FunctionWrapper.get().apply(a1, a2, a3, a4)[0];
                        },
                        outputDataType);
            case 5:
                return udf(
                        (a1, a2, a3, a4, a5) -> {
                            return FunctionWrapper.get().apply(a1, a2, a3, a4, a5)[0];
                        },
                        outputDataType);
            case 6:
                return udf(
                        (a1, a2, a3, a4, a5, a6) -> {
                            return FunctionWrapper.get().apply(a1, a2, a3, a4, a5, a6)[0];
                        },
                        outputDataType);
            case 7:
                return udf(
                        (a1, a2, a3, a4, a5, a6, a7) -> {
                            return FunctionWrapper.get().apply(a1, a2, a3, a4, a5, a6, a7)[0];
                        },
                        outputDataType);
            case 8:
                return udf(
                        (a1, a2, a3, a4, a5, a6, a7, a8) -> {
                            return FunctionWrapper.get().apply(a1, a2, a3, a4, a5, a6, a7, a8)[0];
                        },
                        outputDataType);
            case 9:
                return udf(
                        (a1, a2, a3, a4, a5, a6, a7, a8, a9) -> {
                            return FunctionWrapper.get().apply(a1, a2, a3, a4, a5, a6, a7, a8, a9)[0];
                        },
                        outputDataType);
            case 10:
                return udf(
                        (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) -> {
                            return FunctionWrapper.get().apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)[0];
                        },
                        outputDataType);
            default:
                throw new RuntimeException("UDFs with " + inputColumnNames.length + " args are not supported.");
        }
    }

    public static UserDefinedFunction createFilterFunction(String functionName, String[] inputColumnNames){
        switch(inputColumnNames.length) {
            case 0:
                return udf(
                        () -> {
                            return intToBool(FunctionWrapper.get().apply()[0]);
                        },
                        DataTypes.BooleanType);
            case 1:
                return udf(
                        (a1) -> {
                            return intToBool(FunctionWrapper.get().apply(a1)[0]);
                        },
                        DataTypes.BooleanType);
            case 2:
                return udf(
                        (a1, a2) -> {
                            return intToBool(FunctionWrapper.get().apply(a1, a2)[0]);
                        },
                        DataTypes.BooleanType);
            case 3:
                return udf(
                        (a1, a2, a3) -> {
                            return intToBool(FunctionWrapper.get().apply(a1, a2, a3)[0]);
                        },
                        DataTypes.BooleanType);
            case 4:
                return udf(
                        (a1, a2, a3, a4) -> {
                            return intToBool(FunctionWrapper.get().apply(a1, a2, a3, a4)[0]);
                        },
                        DataTypes.BooleanType);
            case 5:
                return udf(
                        (a1, a2, a3, a4, a5) -> {
                            return intToBool(FunctionWrapper.get().apply(a1, a2, a3, a4, a5)[0]);
                        },
                        DataTypes.BooleanType);
            case 6:
                return udf(
                        (a1, a2, a3, a4, a5, a6) -> {
                            return intToBool(FunctionWrapper.get().apply(a1, a2, a3, a4, a5, a6)[0]);
                        },
                        DataTypes.BooleanType);
            case 7:
                return udf(
                        (a1, a2, a3, a4, a5, a6, a7) -> {
                            return intToBool(FunctionWrapper.get().apply(a1, a2, a3, a4, a5, a6, a7)[0]);
                        },
                        DataTypes.BooleanType);
            case 8:
                return udf(
                        (a1, a2, a3, a4, a5, a6, a7, a8) -> {
                            return intToBool(FunctionWrapper.get().apply(a1, a2, a3, a4, a5, a6, a7, a8)[0]);
                        },
                        DataTypes.BooleanType);
            case 9:
                return udf(
                        (a1, a2, a3, a4, a5, a6, a7, a8, a9) -> {
                            return intToBool(FunctionWrapper.get().apply(a1, a2, a3, a4, a5, a6, a7, a8, a9)[0]);
                        },
                        DataTypes.BooleanType);
            case 10:
                return udf(
                        (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) -> {
                            return intToBool(FunctionWrapper.get().apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)[0]);
                        },
                        DataTypes.BooleanType);
            default:
                throw new RuntimeException("UDFs with " + inputColumnNames.length + " args are not supported.");
        }
    }

    /**
     * Used for performance testing.
     */
    public static UserDefinedFunction createAddFunction() {
        return udf(
                (a1, a2) -> {
                    return (int)a1 + (int)a2;
                },
                DataTypes.IntegerType
        );
    }

    /**
     * Used for performance testing.
     */
    public static UserDefinedFunction createRandomFunction() {
        Random rn = new Random();
        return udf(
                (a1, a2) -> {
                    return (int)a1 + (int)a2 + rn.nextInt(10000);
                },
                DataTypes.IntegerType
        );
    }

    private static boolean intToBool(Object obj) {
        return ((Integer) obj) != 0;
    }
}
