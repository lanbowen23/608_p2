package sql608.helper;

public class ValueContainer<A, B> {
    public final A nodeName;
    public final B condition;

    public ValueContainer(A name, B condition) {
        this.nodeName = name;
        this.condition = condition;
    }
}

