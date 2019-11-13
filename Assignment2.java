import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class Assignment2 {

    public static void main(String[] args) throws Exception {
        Assignment2 a2 = new Assignment2();
        System.out.println("Connected to Database: " + a2.connectDB("jdbc:postgresql://localhost:5432/xucharle", "xucharle", "password"));
        System.out.println("Connection Open: " + !a2.connection.isClosed());
        
        System.out.println("Insert Player: " + a2.insertPlayer(8, "Charles Xu", 8, 3));
        System.out.println("Insert Duplicate Player: " + a2.insertPlayer(8, "Charles Xu", 8, 3));
        System.out.println("Insert Invalid Country: " + a2.insertPlayer(8, "Alex Smith", 8, 10));
        
        System.out.println("Closing Database: " + a2.disconnectDB());
        System.out.println("Connection Open: " + !a2.connection.isClosed());
    }

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
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password) {
        try {
            if (connection != null && !connection.isClosed()) {
                if (!disconnectDB()) {
                    return false;
                }
            }

            connection = DriverManager.getConnection(URL, username, password);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
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
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                ps.close();
            } catch (Exception e) {
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
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            try {
                ps.close();
                rs.close();
            } catch (Exception e) {
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
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        } finally {
            try {
                ps.close();
                rs.close();
                psB.close();
                rsB.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public boolean chgRecord(int pid, int year, int wins, int losses) {
        try {
            ps = connection.prepareStatement("UPDATE record SET year=?, wins=?, losses=? WHERE pid=?;");

            ps.setInt(1, year);
            ps.setInt(2, wins);
            ps.setInt(3, losses);
            ps.setInt(4, pid);

            int count = ps.executeUpdate();

            if (count >= 1) {
                return true;
            } else {
                psB = connection.prepareStatement("INSERT INTO record VALUES (?, ?, ?, ?);");

                psB.setInt(1, pid);
                psB.setInt(2, year);
                psB.setInt(3, wins);
                psB.setInt(4, losses);

                int update = ps.executeUpdate();
                return update >= 1;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                ps.close();
                psB.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public boolean deleteMatchBetween(int p1id, int p2id) {
        try {
            ps = connection.prepareStatement("DELETE FROM event WHERE (winid=? AND lossid=?) OR (winid=? AND lossid=?);");

            ps.setInt(1, p1id);
            ps.setInt(2, p2id);
            ps.setInt(3, p2id);
            ps.setInt(4, p1id);

            int count = ps.executeUpdate();
            return count > 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                ps.close();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    public String listPlayerRanking() {
        StringBuilder sb = new StringBuilder();

        try {
            ps = connection.prepareStatement("SELECT pname, globalrank FROM player ORDER BY globalrank DESC;");
            rs = ps.executeQuery();

            while (rs.next()) {
                if (sb.length() > 0)
                    sb.append("\n");
                String player = String.format("%s:%d", rs.getString("pname"), rs.getInt("globalrank"));
                sb.append(player);
            }

            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        } finally {
            try {
                ps.close();
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
                return "";
            }
        }
    }

    public int findTriCircle() {
        try {
            ps = connection.prepareStatement("SELECT COUNT(*) FROM event e1, event e2, event e3 WHERE e1.winid < e2.winid AND e2.winid < e3.winid AND e1.winid = e3.lossid AND e2.winid = e1.lossid AND e3.winid = e2.lossid;");
            rs = ps.executeQuery();
            return rs.next() ? rs.getInt(1) : 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            try {
                ps.close();
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
                return 0;
            }
        }
    }

    public boolean updateDB() {
        try {
            ps = connection.prepareStatement("DROP TABLE IF EXISTS championPlayers CASCADE;");
            ps.execute();
            ps.close();

            ps = connection.prepareStatement("CREATE TABLE championPlayers (pid INTEGER, pname VARCHAR NOT NULL, nchampions INTEGER);");
            ps.execute();
            ps.close();

            ps = connection.prepareStatement("INSERT INTO championPlayers (SELECT p.pid, p.pname, COUNT(c.tid) AS nchampions FROM player p JOIN champion c ON c.pid = p.pid GROUP BY p.pid);");
            ps.execute();

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                ps.close();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
    }
}
