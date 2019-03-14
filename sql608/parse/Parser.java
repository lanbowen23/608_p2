package sql608.parse;

import sql608.helper.ValueContainer;
import sql608.helper.TableContainer;
import storageManager.Field;
import storageManager.FieldType;
import storageManager.Relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {

    // not converting value inside " "
    private String myLowercase(String sql) {
        // check literal " for STR type value
        if (!sql.contains("\"")) {
            return sql.toLowerCase();
        }
        sql += " ";
        // split on literal "
        String[] records = sql.split("\\\"");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < records.length; i++) {
            if (i % 2 == 0) {
                records[i] = records[i].toLowerCase();
            }
            sb.append(records[i]);
            if (i != records.length - 1) {
                sb.append("\"");
            }
        }
        return sb.toString().trim();
    }

    private void printArr(String[] arr) {
        StringBuilder sb = new StringBuilder();
        for (String str : arr) sb.append(str).append(" ");
        System.out.println(sb.toString());
    }

    public TableContainer<String, ArrayList<String>, ArrayList<FieldType>> parseCreate(String sql) {
        sql = sql.trim().toLowerCase();

        // create table name (...)
        // [\s]+ for whitespace
        String[] splitResult = sql.split("[\\s]+", 4);
        String name = splitResult[2];
        // match ()
        Pattern pattern = Pattern.compile("\\((.+)\\)");
        Matcher matcher = pattern.matcher(splitResult[3]);
        if (!matcher.find()) {
            return null;
        }
        // sid int, homework int
        String[] fieldPairs = matcher.group(1).trim().split("[\\s]*,[\\s]*");
        ArrayList<String> fieldNameList = new ArrayList<>();
        ArrayList<FieldType> fieldTypeList = new ArrayList<>();
        for (String pair : fieldPairs) {
            String[] records = pair.split("[\\s]+");
            FieldType type = (records[1].equals("int")) ? FieldType.INT : FieldType.STR20;
            fieldNameList.add(records[0]);
            fieldTypeList.add(type);
        }
        return new TableContainer<>(name, fieldNameList, fieldTypeList);
    }

    public String parseDrop(String sql) {
        sql = sql.trim().toLowerCase();
        // DROP TABLE course
        String tableName = sql.split("[\\s]+", 4)[2];
        int len = tableName.length();
        if (tableName.charAt(len - 1) == ';') {
            return tableName.substring(0, len - 1);
        }
        return tableName;
    }

    public TableContainer<String, ArrayList<String>, ArrayList<Field>> parseInsertOneTuple(String sql, Relation relation) {
        // INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, "A")
        sql = myLowercase(sql.trim());
        if (!sql.contains("value")) { return null; }
        String[] splitResult = sql.replaceAll("\\\"", "").split("[\\s]+", 4);
        String tableName = splitResult[2];
        if (relation == null) {
            return new TableContainer<>(tableName, null, null);
        }

        // find ()
        Pattern pattern = Pattern.compile("\\((.+?)\\)");
        Matcher matcher = pattern.matcher(splitResult[3]);
        // find the nodeName match for corresponding field names
        if (!matcher.find()) { return null; }
        ArrayList<String> fieldNames = new ArrayList<>();
        String[] nameString = matcher.group(1).split(",");
        for (String str : nameString) {
            String name = str.trim();
            fieldNames.add(name);
        }
        // condition match
        // VALUES (1, 99, 100, 100, "A")
        matcher.find();
        ArrayList<Field> values = new ArrayList<>();
        String[] valuesString = matcher.group(1).split(",");
        int i = 0;
        for (String str : valuesString) {
            Field field = new Field();
            str = str.trim();
            if (relation.getSchema().getFieldType(fieldNames.get(i)) == FieldType.INT)
            {
                field.type = FieldType.INT;
                if (str.equals("null")) {
                    field.integer = Integer.MIN_VALUE;
                } else {
                    field.integer = Integer.valueOf(str);
                }
            } else {
                field.type = FieldType.STR20;
                field.str = str;
            }
            values.add(field);
            i++;
        }
        return new TableContainer<>(tableName, fieldNames, values);
    }

    public ValueContainer<String, ParserContainer> parseInsertWithSelect(String sql) {
        sql = sql.trim().toLowerCase();
        String[] splitResult = sql.split("[\\s]+", 4);
        String tableName = splitResult[2];
        String selectCondition = splitResult[3];
        ParserContainer parserContainer = parseSelect(selectCondition);
        return new ValueContainer<>(tableName, parserContainer);
    }

    public ParserContainer parseSelect(String sql) {
        sql = sql.trim().toLowerCase();
        if (!sql.contains("from")) { return null; }
        /* change "A" in VALUES to A; replace quotation mark */
        sql = sql.replaceAll("[\\s]+", " ").replaceAll("\\\"", "");
        sql = sql.replaceAll("\\[", "(").replaceAll("]", ")");

        String[] splitResult = sql.split("select|from|where|order by");
        /* In ATTR part: replace comma by whitespace , and split by whitespace */
        String[] attributes = splitResult[1].trim().replaceAll("[\\s]*,[\\s]*", " ").split("\\s");

        /* get rid of DISTINCT by move right one step */
        boolean isDistinct = false;
        if (attributes[0].equals("distinct")) isDistinct = true;
        String[] attributesTemp = new String[attributes.length - 1];
        if (isDistinct) {
            System.arraycopy(attributes, 1, attributesTemp, 0, attributesTemp.length);
            attributes = attributesTemp;
        }

        /* FROM tables */
        String[] tablesName = splitResult[2].trim().split("[\\s]*,[\\s]*");

        /* WHERE condition */
        String condition = "";
        if (splitResult.length > 3) {
            condition = (splitResult.length == 5) ?
                    splitResult[3] + " order by " + splitResult[4]
                    : (!sql.contains("order by")) ? splitResult[3] : "order by" + splitResult[3];
            condition = condition.trim();
        }

        /* setTable or setTables */
        if (tablesName.length == 1) {
            return parseSelectSingleTable(attributes, isDistinct, tablesName[0], condition);
        } else {
            return parseSelectMulTables(attributes, isDistinct, tablesName, condition);
        }
    }

    private ParserContainer parseSelectBase(String[] attributes, boolean isDistinct, String condition) {
        ParserContainer parserContainer = new ParserContainer();
        parserContainer.setDistinct(isDistinct);
        parserContainer.setAttributes(new ArrayList<>(Arrays.asList(attributes)));
        parserContainer.setFrom(true);

        /* WHERE condition with and without ORDER BY*/
        if (condition.length() != 0 && condition.indexOf("order by") != 0) {
            parserContainer.setWhere(true);
            if (condition.contains(" order by ")) {
                String[] split = condition.split(" order by ");
                parserContainer.setOrder(true);
                parserContainer.setOrderAttribute(split[1].trim());
                parserContainer.setConditions(split[0].trim());
            } else {
                parserContainer.setConditions(condition);
            }
        } else
            /* only has ORDER BY, no WHERE */
            if (condition.contains("order by ")) {
            String[] split = condition.split("order by ");
            parserContainer.setOrder(true);
            parserContainer.setOrderAttribute(split[1].trim());
        }
        return parserContainer;
    }

    private ParserContainer parseSelectSingleTable(String[] attributes, boolean isDistinct, String table, String condition) {
        printArr(attributes);
        ParserContainer parserContainer = parseSelectBase(attributes, isDistinct, condition);
        parserContainer.setTable(table);
        return parserContainer;
    }

    private ParserContainer parseSelectMulTables(String[] attributes, boolean isDistinct, String[] tables,
                                                 String condition) {
        printArr(attributes);
        printArr(tables);
        ParserContainer parserContainer = parseSelectBase(attributes, isDistinct, condition);
        parserContainer.setTables(new ArrayList<>(Arrays.asList(tables)));
        return parserContainer;
    }

    public ParserContainer parseDelete(String sql) {
        sql = myLowercase(sql.trim());
        sql = sql.replaceAll("\\\"", "");
        ParserContainer parserContainer = new ParserContainer();
        parserContainer.setFrom(true);
        if (sql.contains("where")) {
            parserContainer.setWhere(true);
            String splitResult[] = sql.split("from|where");
            parserContainer.setTable(splitResult[1].trim());
            parserContainer.setConditions(splitResult[2].trim());
        } else {
            parserContainer.setWhere(false);
            String splitResult[] = sql.split("from");
            parserContainer.setTable(splitResult[1].trim());
        }
        return parserContainer;
    }

}
