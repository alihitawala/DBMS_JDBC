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

public class TestConnection {
    BufferedReader br = null;
    Scanner sc = null;
    String url = "jdbc:postgresql://stampy.cs.wisc.edu/cs564instr?sslfactory=org.postgresql.ssl.NonValidatingFactory&ssl";
    Connection conn;
    Random random;

    public TestConnection() throws SQLException {
        sc = new Scanner(System.in);
        br = new BufferedReader(new InputStreamReader(System.in));
        conn = DriverManager.getConnection(url);
        random = new Random(System.currentTimeMillis());
    }

    public void execute() throws SQLException {
        while(true) {
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
        }
    }

    @Override
    protected void finalize() throws Throwable {
        conn.close();
        super.finalize();
    }

    public static void main(String[] args) throws Exception {
        new TestConnection().executeTest();
//        System.out.println(new TestConnection().sampleMfromN(10,11));
        new TestConnection().execute();
    }

    private void executeTest() throws SQLException {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select count(*) from hw3.sales");
        while (rs.next()) {
            System.out.print("Row returned: ");
            System.out.println(rs.getInt(1));
        }
        st.close();
        rs.close();
    }

    private void resetSeed() {

    }

    private void rawQuery() {

    }

    private void tableQuery() {
        System.out.print("Enter table name: ");
        String tableName = "";
        try {
            tableName = br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in taking input!");
        }
        int n = this.askForSampleNumber();
        int N;
        try {
            N = this.getRowCount(tableName);
        }
        catch (Exception ex) {
            throw new RuntimeException("Table not found!");
        }
        List<Integer> rows = this.sampleMfromN(n, N);
        String inList = getInList(rows);
        try {
            executeQuery("select * from hw3." + tableName, inList);
        } catch (Exception ex) {
            throw new RuntimeException("Error in SQL query!");
        }
    }

    private void executeQuery(String rawQuery, String inList) throws SQLException {
        Statement st = conn.createStatement();
        String query = constructQuery(rawQuery, inList);
        ResultSet rs = st.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 2; i <= columnsNumber; i++) {
            System.out.print(rsmd.getColumnName(i) + "\t");
        }
        System.out.println();
        while (rs.next()) {
            for (int i = 2; i <= columnsNumber; i++) {
//                if (i > 1) System.out.print(",  ");
                String columnValue = rs.getString(i);
                System.out.print(columnValue + "\t");
            }
            System.out.println("");
        }
        st.close();
        rs.close();
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

    private int getRowCount(String tableName) throws SQLException {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select count(*) from hw3." + tableName);
        if (rs.next()) {
            return rs.getInt(1);
        }
        throw new RuntimeException("SQL error!");
    }

    private int askForSampleNumber() {
        System.out.print("Enter the sample rows you want : ");
        return sc.nextInt();
    }

    private void showOptions() {
        System.out.println("Please enter the options:");
        System.out.println("1. Enter table name for sampling");
        System.out.println("2. Enter raw query");
        System.out.println("3. Reset seed");
        System.out.println("default. Exit");
    }
}