import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Assignment2 {

    // A connection to the database
    Connection connection;

    // Statement to run queries
    Statement sql;

    // Prepared Statement
    PreparedStatement ps;
    PreparedStatement psB;

    // Resultset for the query
    ResultSet rs;
    ResultSet rsB;

    // CONSTRUCTOR
    Assignment2() {
        try {
            Class.forName("com.mysql.jdbc.Driver"); // <= "com.mysql.cj.jdbc.Driver" ?
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password) {
        try {
            connection = DriverManager.getConnection(URL, username, password);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    // Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB() {
        try {
            connection.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean insertPlayer(int pid, String pname, int globalRank, int cid) {
        try {
            ps = connection.prepareStatement("INSERT INTO player VALUES (?, ?, ?, ?);");

            ps.setInt(1, pid);
            ps.setString(2, pname);
            ps.setInt(3, globalRank);
            ps.setInt(4, cid);

            int count = ps.executeUpdate();
            return count == 1;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public int getChampions(int pid) {
        try {
            ps = connection.prepareStatement("SELECT COUNT(*) FROM champion WHERE pid=?;");

            ps.setInt(1, pid);

            rs = ps.executeQuery();

            return rs.next() ? rs.getInt(1) : 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return 0;
        } finally {
            try {
                ps.close();
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public String getCourtInfo(int courtid) {
        try {
            ps = connection.prepareStatement("SELECT * FROM court WHERE courtid=?;");

            ps.setInt(1, courtid);

            rs = ps.executeQuery();

            if (rs.next()) {
                String cname = rs.getString("courtname");
                int capacity = rs.getInt("capacity");
                int tid = rs.getInt("tid");

                psB = connection.prepareStatement("SELECT * FROM tournament WHERE tid=?;");
                psB.setInt(1, tid);
                rsB = psB.executeQuery();

                if (rsB.next()) {
                    String tname = rsB.getString("tname");
                    return String.format("%d:%s:%d:%s", courtid, cname, capacity, tname);
                } else {
                    return "";
                }
            } else {
                return "";
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return "";
        } finally {
            try {
                ps.close();
                rs.close();
                psB.close();
                rsB.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean chgRecord(int pid, int year, int wins, int losses) {
        return false;
    }

    public boolean deleteMatcBetween(int p1id, int p2id) {
        return false;
    }

    public String listPlayerRanking() {
        return "";
    }

    public int findTriCircle() {
        return 0;
    }

    public boolean updateDB() {
        /* 
         * DROP TABLE IF EXISTS championPlayers CASCADE;
         * 
         * CREATE TABLE player(
               pid         INTEGER     PRIMARY KEY,
               pname       VARCHAR     NOT NULL,
               nchampions  INTEGER     NOT NULL
           );
        */ 
        return false;
    }
}
