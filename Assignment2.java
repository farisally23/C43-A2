import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class Assignment2 {

    public static void main(String[] args) throws Exception {

        // psql < a2.ddl
        // psql < testData.sql
        
        Assignment2 a2 = new Assignment2();

        System.out.printf("%b\t%s\n", a2.connectDB("jdbc:postgresql://localhost:5432/xucharle", "xucharle", "password"), "Connect to database");
        System.out.printf("%b\t%s\n", !a2.connection.isClosed(), "Connection is open");

        System.out.printf("%b\t%s\n", a2.insertPlayer(8, "Charles Xu", 8, 3), "Insert player new");
        System.out.printf("%b\t%s\n", !a2.insertPlayer(8, "Charles Xu", 8, 3), "Insert player duplicate");
        System.out.printf("%b\t%s\n", !a2.insertPlayer(8, "Alex Smith", 9, 3), "Insert player duplicate id");
        System.out.printf("%b\t%s\n", !a2.insertPlayer(9, "Alex Smith", 9, 10), "Insert player invalid country id");

        System.out.printf("%b\t%s\n", a2.getChampions(1) == 6, "Champion status valid player");
        System.out.printf("%b\t%s\n", a2.getChampions(100) == 0, "Champion status invalid player");

        System.out.printf("%b\t%s\n", a2.getCourtInfo(10).equals("10:Court10:500:Big Tournament"), "Court status valid court");
        System.out.printf("%b\t%s\n", a2.getCourtInfo(100).equals(""), "Court status invalid court");

        System.out.printf("%b\t%s\n", a2.chgRecord(1, 2011, 25, 25), "Change record valid player");
        System.out.printf("%b\t%s\n", !a2.chgRecord(100, 2011, 25, 25), "Change record invalid player");
        System.out.printf("%b\t%s\n", !a2.chgRecord(1, 2015, 25, 25), "Change record invalid year");

        System.out.printf("%b\t%s\n", a2.deleteMatchBetween(1, 5), "Delete matches valid user");
        System.out.printf("%b\t%s\n", a2.deleteMatchBetween(6, 1), "Delete matches valid user reverse");
        System.out.printf("%b\t%s\n", !a2.deleteMatchBetween(1, 6), "Delete matches invalid users");

        int circles = a2.findTriCircle();
        System.out.printf("%b\t%s\n", circles == 2, circles + " = 2 circles (1, 2, 3) & (2, 3, 4)");

        System.out.printf("%b\t%s\n", a2.updateDB(), "Update database");

        System.out.println("== Player Rankings:\n" + a2.listPlayerRanking());

        System.out.printf("%b\t%s\n", a2.disconnectDB(), "Closing database");
        System.out.printf("%b\t%s\n", a2.connection.isClosed(), "Connection is closed");
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
            return false;
        }
    }

    public boolean insertPlayer(int pid, String pname, int globalRank, int cid) {
        try {
            ps = connection.prepareStatement("INSERT INTO A2.player VALUES (?, ?, ?, ?);");

            ps.setInt(1, pid);
            ps.setString(2, pname);
            ps.setInt(3, globalRank);
            ps.setInt(4, cid);

            int count = ps.executeUpdate();
            return count == 1;
        } catch (Exception e) {
            return false;
        } finally {
            try {
                ps.close();
            } catch (Exception e) {
                return false;
            }
        }
    }

    public int getChampions(int pid) {
        try {
            ps = connection.prepareStatement("SELECT COUNT(*) FROM A2.champion WHERE pid=?;");

            ps.setInt(1, pid);

            rs = ps.executeQuery();

            return rs.next() ? rs.getInt(1) : 0;
        } catch (Exception e) {
            return 0;
        } finally {
            try {
                ps.close();
                rs.close();
            } catch (Exception e) {
                return 0;
            }
        }
    }

    public String getCourtInfo(int courtid) {
        try {
            ps = connection.prepareStatement("SELECT * FROM A2.court WHERE courtid=?;");

            ps.setInt(1, courtid);

            rs = ps.executeQuery();

            if (rs.next()) {
                String cname = rs.getString("courtname");
                int capacity = rs.getInt("capacity");
                int tid = rs.getInt("tid");

                psB = connection.prepareStatement("SELECT * FROM A2.tournament WHERE tid=?;");
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
            return "";
        } finally {
            try {
                ps.close();
                rs.close();
                psB.close();
                rsB.close();
            } catch (Exception e) {
                return "";
            }
        }
    }

    public boolean chgRecord(int pid, int year, int wins, int losses) {
        try {
            ps = connection.prepareStatement("UPDATE A2.record SET wins=?, losses=? WHERE pid=? AND year=?;");

            ps.setInt(1, wins);
            ps.setInt(2, losses);
            ps.setInt(3, pid);
            ps.setInt(4, year);

            int count = ps.executeUpdate();
            return count >= 1;
        } catch (Exception e) {
            return false;
        } finally {
            try {
                ps.close();
            } catch (Exception e) {
                return false;
            }
        }
    }

    public boolean deleteMatchBetween(int p1id, int p2id) {
        try {
            ps = connection.prepareStatement("DELETE FROM A2.event WHERE (winid=? AND lossid=?) OR (winid=? AND lossid=?);");

            ps.setInt(1, p1id);
            ps.setInt(2, p2id);
            ps.setInt(3, p2id);
            ps.setInt(4, p1id);

            int count = ps.executeUpdate();
            return count > 0;
        } catch (Exception e) {
            return false;
        } finally {
            try {
                ps.close();
            } catch (Exception e) {
                return false;
            }
        }
    }

    public String listPlayerRanking() {
        StringBuilder sb = new StringBuilder();

        try {
            ps = connection.prepareStatement("SELECT pname, globalrank FROM A2.player ORDER BY globalrank ASC;");
            rs = ps.executeQuery();

            while (rs.next()) {
                if (sb.length() > 0)
                    sb.append("\n");
                String player = String.format("%s:%d", rs.getString("pname"), rs.getInt("globalrank"));
                sb.append(player);
            }

            return sb.toString();
        } catch (Exception e) {
            return "";
        } finally {
            try {
                ps.close();
                rs.close();
            } catch (Exception e) {
                return "";
            }
        }
    }

    public int findTriCircle() {
        try {
            ps = connection.prepareStatement("SELECT COUNT(*) FROM (SELECT DISTINCT e1.winid, e2.winid, e3.winid FROM A2.event e1, A2.event e2, A2.event e3 WHERE e1.winid < e2.winid AND e2.winid < e3.winid AND e1.winid = e3.lossid AND e2.winid = e1.lossid AND e3.winid = e2.lossid) scope;");
            rs = ps.executeQuery();
            return rs.next() ? rs.getInt(1) : 0;
        } catch (Exception e) {
            return 0;
        } finally {
            try {
                ps.close();
                rs.close();
            } catch (Exception e) {
                return 0;
            }
        }
    }

    public boolean updateDB() {
        try {
            ps = connection.prepareStatement("DROP TABLE IF EXISTS A2.championPlayers CASCADE;");
            ps.execute();
            ps.close();

            ps = connection.prepareStatement("CREATE TABLE A2.championPlayers (pid INTEGER, pname VARCHAR NOT NULL, nchampions INTEGER);");
            ps.execute();
            ps.close();

            ps = connection.prepareStatement("INSERT INTO A2.championPlayers (SELECT p.pid, p.pname, COUNT(c.tid) AS nchampions FROM A2.player p JOIN A2.champion c ON c.pid = p.pid GROUP BY p.pid);");
            ps.execute();

            return true;
        } catch (Exception e) {
            return false;
        } finally {
            try {
                ps.close();
            } catch (Exception e) {
                return false;
            }
        }
    }
}
