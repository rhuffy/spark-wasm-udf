public enum Operation {
    MAP, FILTER;

    public static Operation from(String str){
        switch (str){
            case "MAP": return MAP;
            case "FILTER": return FILTER;
            default:
                throw new IllegalArgumentException("Invalid Operation: " + str);
        }
    }
}
