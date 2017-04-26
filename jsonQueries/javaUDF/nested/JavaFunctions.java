package javaUDF;

public class JavaFunctions {
    public static Integer square(Integer x) {
        return x * x;
    }

    public static Integer triple(Integer x) {
        return 3 * x;
    }

    public static Integer negate(Integer x) {
        return -1 * x;
    }

    public static int identity(int x) {
        return x;
    }
}
