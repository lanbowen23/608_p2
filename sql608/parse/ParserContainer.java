package sql608.parse;

import java.util.ArrayList;

/*
string snippet for parsed results
store useful boolean flags and values to direct the join and sort operation
*/
public class ParserContainer {
    // flags for has such parts or not
    private boolean distinct;
    private boolean from;
    private boolean where;
    private boolean order;
    // parse to formal strings
    private String conditions;
    private String orderAttribute;
    private ArrayList<String> attributes;

    private String table;  // for single table case
    private ArrayList<String> tables;  // for multiple table case

    public ParserContainer() { }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public boolean isWhere() {
        return where;
    }

    public void setWhere(boolean where) {
        this.where = where;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    public String getConditions() {
        return conditions;
    }

    public void setConditions(String conditions) {
        this.conditions = conditions;
    }

    public ArrayList<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(ArrayList<String> attributes) {
        this.attributes = attributes;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public ArrayList<String> getTables() {
        return tables;
    }

    public void setTables(ArrayList<String> tables) {
        this.tables = tables;
    }

    public boolean isFrom() {
        return from;
    }

    public void setFrom(boolean from) {
        this.from = from;
    }

    public String getOrderAttribute() {
        return orderAttribute;
    }

    public void setOrderAttribute(String orderAttribute) {
        this.orderAttribute = orderAttribute;
    }
}
