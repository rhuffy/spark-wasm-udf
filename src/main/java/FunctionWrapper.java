import org.wasmer.exports.Function;

public class FunctionWrapper {
    private static Function function;
    public static Function get() {
        return function;
    }
    public static void set(Function function) {
        FunctionWrapper.function = function;
    }
}
