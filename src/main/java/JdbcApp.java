/**
 * Created by alihitawala on 10/30/16.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class JdbcApp {
    BufferedReader br = null;
    Scanner sc = null;
    String url = "jdbc:postgresql://stampy.cs.wisc.edu/cs564instr?sslfactory=org.postgresql.ssl.NonValidatingFactory&ssl";
    Connection conn;
    Random random;

    public JdbcApp() throws SQLException {
        sc = new Scanner(System.in);
        br = new BufferedReader(new InputStreamReader(System.in));
        conn = DriverManager.getConnection(url);
        random = new Random(System.currentTimeMillis());
    }

    public static void main(String[] args) throws Exception {
//        new JdbcApp().setSchema();
        new JdbcApp().execute();
    }

    private void setSchema() throws SQLException {
        Statement st = conn.createStatement();
        st.execute("set search_path to '" + "hw3" + "'");
        st.close();
    }

    private void execute() throws SQLException {
        this.setSchema();
        while(true) {
            try {
                showOptions();
                int opt = this.sc.nextInt();
                switch (opt) {
                    case 1:
                        tableQuery();
                        break;
                    case 2:
                        rawQuery();
                        break;
                    case 3:
                        resetSeed();
                        break;
                    default:
                        System.exit(0);
                }
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        conn.close();
        super.finalize();
    }

    private void resetSeed() {
        System.out.print("Enter new seed : ");
        this.random.setSeed(sc.nextInt());
    }

    private void rawQuery() {
        System.out.print("Enter query (only select): ");
        String query = "";
        try {
            query = br.readLine().trim().toLowerCase();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in taking input!");
        }
        this.sampleAndRunQuery(query);
    }

    private void tableQuery() {
        System.out.print("Enter table name: ");
        String tableName = "";
        try {
            tableName = br.readLine().trim().toLowerCase();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in taking input!");
        }
        String rawQuery = "select * from hw3." + tableName;
        this.sampleAndRunQuery(rawQuery);
    }

    private void sampleAndRunQuery(String rawQuery) {
        int n = this.askForSampleNumber();
        int N;
        try {
            N = this.getRowCount(rawQuery);
        }
        catch (Exception ex) {
            throw new RuntimeException("SQL Error : error in query check table name and associated schema to fix!");
        }
        List<Integer> rows = this.sampleMfromN(n, N);
        String inList = getInList(rows);
        try {
            executeQuery(rawQuery, inList);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    private void executeQuery(String rawQuery, String inList) throws SQLException {
        Statement st = conn.createStatement();
        String query = constructQuery(rawQuery, inList);
        ResultSet rs;
        try {
            rs = st.executeQuery(query);
        } catch (Exception ex) {
            throw new RuntimeException("Error in SQL query!");
        }
        ResultSetMetaData rsmd = rs.getMetaData();
        boolean isStdOut = isStdOut();
        if (isStdOut) {
            int columnsNumber = rsmd.getColumnCount();
            for (int i = 2; i <= columnsNumber; i++) {
                System.out.print(rsmd.getColumnName(i) + "\t");
            }
            System.out.println();
            while (rs.next()) {
                for (int i = 2; i <= columnsNumber; i++) {
                    String columnValue = rs.getString(i);
                    System.out.print(columnValue + "\t");
                }
                System.out.println("");
            }
        } else {
            System.out.print("Enter new table name: ");
            String tableName = "";
            try {
                tableName = br.readLine().trim().toLowerCase();
            } catch (IOException e) {
                throw new RuntimeException("Error in taking input!");
            }
            try {
                this.createNewTable(tableName, rsmd);
                this.insertDataIntoNewTable(tableName, rs);
            } catch (Exception ex) {
                throw new RuntimeException("Error in creating new table!");
            }
        }
        st.close();
        rs.close();
    }

    private void insertDataIntoNewTable(String tableName, ResultSet rs) throws SQLException {
        String insertQ = "insert into " + tableName + " values ";
        Statement st = conn.createStatement();
        while (rs.next()) {
            StringBuilder builder = new StringBuilder("(");
            String delim = "";
            ResultSetMetaData metaData = rs.getMetaData();
            for (int i = 2; i <= metaData.getColumnCount(); i++) {
                String columnValue = rs.getString(i);
                String type = metaData.getColumnTypeName(i);
                if (type.contains("char") || type.contains("date") || type.contains("time") || type.contains("bool")) {
                    columnValue = "'" + columnValue + "'";
                }
                builder.append(delim).append(columnValue);
                delim = ",";
            }
            builder.append(")");
            st.executeUpdate(insertQ + builder.toString());
        }
        st.close();
    }

    private void createNewTable(String tableName, ResultSetMetaData rsmd) throws SQLException {
        String createQuery = this.getCreateQuery(tableName, rsmd);
        Statement st = conn.createStatement();
        st.execute(createQuery);
    }

    private String getCreateQuery(String tableName, ResultSetMetaData rsmd) throws SQLException {
        StringBuilder builder = new StringBuilder("");
        builder.append("Create table ").append(tableName).append(" ( ");
        String delim = "";
        for (int i = 2; i <= rsmd.getColumnCount(); i++) {
            builder.append(delim).append(rsmd.getColumnName(i)).append(" ").append(rsmd.getColumnTypeName(i));
            delim = ",";
        }
        builder.append(" )");
        return builder.toString();
    }

    private String constructQuery(String rawQuery, String inList) {
        if (!rawQuery.substring(0, 6).equals("select")) {
           return rawQuery;
        }
        //select * from (select row_number() over (order by (select 1)) as r, * from hw3.sales) as T where r in (1,2,9,10);
        StringBuilder builder = new StringBuilder("");
        String rn = " row_number() over (order by (select 1)) as r,";
        builder.append("select * from (").append(rawQuery.substring(0,6)).append(rn).append(rawQuery.substring(6));
        builder.append(") as T where r in ").append(inList);
        return builder.toString();
    }

    private String getInList(List<Integer> rows) {
        StringBuilder builder = new StringBuilder("(");
        String prefix = "";
        for (Integer row : rows) {
            builder.append(prefix);
            prefix = ",";
            builder.append(row);
        }
        return builder.append(")").toString();
    }

    private List<Integer> sampleMfromN(int n, int N) {
        List<Integer> list = new ArrayList<Integer>();
        if (n>=N) {
            for (int i = 1; i <=N ; i++) {
                list.add(i);
            }
            return list;
        }
        int t=0, i=1, m=0;
        while (n>m) {
            double u = random.nextDouble();
            if ((N - t) * u < n - m) {
                list.add(i);
                m++;
            }
            i++;
            t++;
        }
        return list;
    }

    private int getRowCount(String query) throws SQLException {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select count(*) from (" + query + ") as T");
        if (rs.next()) {
            return rs.getInt(1);
        }
        throw new RuntimeException("SQL error!");
    }

    private int askForSampleNumber() {
        System.out.print("Enter the sample rows you want : ");
        int i = sc.nextInt();
        if (i<=0) {
            throw new RuntimeException("Enter the sample number of rows greater than 0!");
        }
        return i;
    }

    private void showOptions() {
        System.out.println("Please enter the action number you want to perform:");
        System.out.println("1. Enter table name for sampling");
        System.out.println("2. Enter raw query");
        System.out.println("3. Reset seed");
        System.out.println("default. Exit");
    }

    public boolean isStdOut() {
        System.out.print("Do you want to create a new table for the output of this query? (yes/no) : ");
        String yes;
        try {
            yes = br.readLine().trim().toLowerCase();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in taking input!");
        }
        return !yes.equals("yes");
    }
}