package sql608;

import sql608.helper.three;
import sql608.helper.two;
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

    private void printArr(String prefix, String[] arr) {
        StringBuilder sb = new StringBuilder();
        for (String str : arr) {
            sb.append(str).append(" ");
        }
        System.out.println(sb.toString());
    }

    public three<String, ArrayList<String>, ArrayList<FieldType>> parseCreate(String sql) {
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
        return new three<>(name, fieldNameList, fieldTypeList);
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

    public three<String, ArrayList<String>, ArrayList<Field>> parseInsertOneTuple(String sql, Relation relation) {
        // INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, "A")
        sql = myLowercase(sql.trim());
        if (!sql.contains("value")) { return null; }
        String[] splitResult = sql.replaceAll("\\\"", "").split("[\\s]+", 4);
        String tableName = splitResult[2];
        if (relation == null) {
            return new three<>(tableName, null, null);
        }

        // find ()
        Pattern pattern = Pattern.compile("\\((.+?)\\)");
        Matcher matcher = pattern.matcher(splitResult[3]);
        // find the first match for corresponding field names
        if (!matcher.find()) { return null; }
        ArrayList<String> fieldNames = new ArrayList<>();
        String[] nameString = matcher.group(1).split(",");
        for (String str : nameString) {
            String name = str.trim();
            fieldNames.add(name);
        }
        // second match
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
        return new three<>(tableName, fieldNames, values);
    }

    public two<String, ParserTree> parseInsertForSelect(String sql) {
        sql = sql.trim().toLowerCase();
        String[] splitResult = sql.split("[\\s]+", 4);
        String tableName = splitResult[2];
        String selectCondition = splitResult[3];
        ParserTree parserTree = parseSelect(selectCondition);
        return new two<String, ParserTree>(tableName, parserTree);
    }

    public ParserTree parseSelect(String sql) {
        sql = sql.trim().toLowerCase();
        if (!sql.contains("from")) { return null; }
        sql = sql.replaceAll("[\\s]+", " ").replaceAll("\\\"", "");

        String[] splitResult = sql.split("select|from|where|order by");
        // replace literal , and split by whitespace
        // select attributes (columns)
        String[] attributes = splitResult[1].trim().replaceAll("[\\s]*,[\\s]*", " ").split("\\s");
        boolean isDistinct = false;
        if (attributes[0].equals("distinct")) { isDistinct = true; }
        // get rid of distinct
        String[] attributesTemp = new String[attributes.length - 1];
        if (isDistinct) {
            for (int i = 0; i < attributesTemp.length; i++) {
                attributesTemp[i] = attributes[i + 1];
            }
            attributes = attributesTemp;
        }
        // from
        String[] tablesName = splitResult[2].trim().split("[\\s]*,[\\s]*");
        // where
        String condition = "";
        if (splitResult.length > 3) {
            condition = (splitResult.length == 5) ?
                    splitResult[3] + " order by " + splitResult[4]
                    : (!sql.contains("order by")) ? splitResult[3] : "order by" + splitResult[3];
            condition = condition.trim();
        }

        if (tablesName.length == 1) {
            return parseSelectSingleTable(attributes, isDistinct, tablesName[0], condition);
        } else {
            return parseSelectMulTables(attributes, isDistinct, tablesName, condition);
        }
    }

    private ParserTree parseSelectBase(String[] attributes, boolean isDistinct, String condition) {
        ParserTree tree = new ParserTree("select");
        tree.setDistinct(isDistinct);
        tree.setFrom(true);
        tree.setAttributes(new ArrayList<>(Arrays.asList(attributes)));

        if (condition.length() != 0 && condition.indexOf("order by") != 0) {
            tree.setWhere(true);
            if (condition.contains(" order by ")) {
                String[] split = condition.split(" order by ");
                tree.setOrder(true);
                tree.setOrderAttribute(split[1].trim());
                tree.setWhereCondition(split[0].trim());
            } else {
                tree.setWhereCondition(condition);
            }
        } else if (condition.contains("order by ")) {
            String[] split = condition.split("order by ");
            tree.setOrder(true);
            tree.setOrderAttribute(split[1].trim());
        }
        return tree;
    }

    private ParserTree parseSelectSingleTable(String[] attributes, boolean isDistinct, String table, String condition) {
        printArr("Attributes: ", attributes);
        ParserTree tree = parseSelectBase(attributes, isDistinct, condition);
        tree.setTable(table);
        return tree;
    }

    private ParserTree parseSelectMulTables(String[] attributes, boolean isDistinct, String[] tables,
                                            String condition) {
        printArr("Attributes: ", attributes);
        printArr("Table names: ", tables);
        ParserTree tree = parseSelectBase(attributes, isDistinct, condition);
        tree.setTables(new ArrayList<>(Arrays.asList(tables)));
        return tree;
    }

    public ParserTree parseDelete(String stmt) {
        stmt = myLowercase(stmt.trim());
        stmt = stmt.replaceAll("\\\"", "");
        ParserTree parserTree = new ParserTree("delete");
        parserTree.setFrom(true);
        if (stmt.contains("where")) {
            parserTree.setWhere(true);
            String splitResult[] = stmt.split("from|where");
            parserTree.setTable(splitResult[1].trim());
            parserTree.setWhereCondition(splitResult[2].trim());
        } else {
            parserTree.setWhere(false);
            String splitResult[] = stmt.split("from");
            parserTree.setTable(splitResult[1].trim());
        }
        return parserTree;
    }


}
