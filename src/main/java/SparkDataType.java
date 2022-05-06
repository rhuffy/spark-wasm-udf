public enum SparkDataType {
    INTEGER,
    FLOAT,
    STRING,
    BOOLEAN;

    public static SparkDataType from(String str) {
        switch (str){
            case "INTEGER": return INTEGER;
            case "FLOAT": return FLOAT;
            case "STRING": return STRING;
            case "BOOLEAN": return BOOLEAN;
            default:
                throw new IllegalArgumentException("Invalid DataType: " + str);
        }
    }
}
