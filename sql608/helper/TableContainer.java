package sql608.helper;

// For Insertion parsed result
// table name, field name list, Field list(values with TYPE(int, str))
public class TableContainer<A, B, C> {
    public final A tableName;
    public final B fieldList;
    public final C fieldType;

    public TableContainer(A tableName, B fieldList, C fieldType) {
        this.tableName = tableName;
        this.fieldList = fieldList;
        this.fieldType = fieldType;
    }
}
