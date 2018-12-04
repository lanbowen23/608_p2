package sql608;

import java.util.ArrayList;

// store useful flags and string snippet for parsed results
public class ParserContainer {
    private String keyword;
    // flags for has such parts or not
    private boolean distinct;
    private boolean where;
    private boolean from;
    private boolean order;
    // parse to formal strings
    private String whereCondition;
    private String table;
    private String orderAttribute;
    private ArrayList<String> tables;
    private ArrayList<String> attributes;

    public ParserContainer(String keyword) {
        this.keyword = keyword;
    }

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

    public String getWhereCondition() {
        return whereCondition;
    }

    public void setWhereCondition(String whereCondition) {
        this.whereCondition = whereCondition;
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
