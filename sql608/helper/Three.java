package sql608.helper;

// For Insertion parsed result
// table name, field name list, Field list(values with TYPE(int, str))
public class Three<A, B, C> {
    public final A first;
    public final B second;
    public final C third;

    public Three(A first, B second, C third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }
}
