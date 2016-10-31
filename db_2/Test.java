/**
 * Created by alihitawala on 10/30/16.
 */
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
public class TestConnection {
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://stampy.cs.wisc.edu/cs564instr?sslfactory = org.postgresql.ssl.NonValidatingFactory&ssl";
        Connection conn = DriverManager.getConnection(url);
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select count(*) from sales");
        while (rs.next()) {
            System.out.print("Row returned: ");
            System.out.println(rs.getInt(1));
        }
// close up shop
        rs.close();
        st.close();
        conn.close();
    }
}
