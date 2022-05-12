import org.wasmer.Instance;

public class InstanceWrapper {
    private static Instance instance;
    public static Instance get(){
        return instance;
    }
    public static void set(Instance instance) {
        InstanceWrapper.instance = instance;
    }
}
