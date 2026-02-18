import java.sql.*;

public class OracleProbe {
  private static void runQuery(Connection conn, String sql) {
    System.out.println("\n--- " + sql);
    try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
      ResultSetMetaData md = rs.getMetaData();
      int cols = md.getColumnCount();
      int rowCount = 0;
      while (rs.next() && rowCount < 5) {
        rowCount++;
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= cols; i++) {
          if (i > 1) sb.append(" | ");
          sb.append(md.getColumnLabel(i)).append("=").append(rs.getString(i));
        }
        System.out.println(sb);
      }
      if (rowCount == 0) {
        System.out.println("(no rows)");
      } else if (rowCount == 5 && rs.next()) {
        System.out.println("(truncated)");
      }
    } catch (SQLException e) {
      System.out.println("SQL ERROR: code=" + e.getErrorCode() + " state=" + e.getSQLState() + " msg=" + e.getMessage());
    }
  }

  private static void runStatement(Connection conn, String sql) {
    System.out.println("\n--- (exec) " + sql);
    try (Statement st = conn.createStatement()) {
      st.execute(sql);
      System.out.println("OK");
    } catch (SQLException e) {
      System.out.println(
          "SQL ERROR: code=" + e.getErrorCode() + " state=" + e.getSQLState() + " msg=" + e.getMessage());
    }
  }

  private static boolean hasArg(String[] args, String arg) {
    for (String a : args) {
      if (arg.equals(a)) return true;
    }
    return false;
  }

  public static void main(String[] args) throws Exception {
    String url = System.getenv().getOrDefault("ORACLE_URL", "jdbc:oracle:thin:@//host.docker.internal:1521/ESWTEST");
    String user = System.getenv().getOrDefault("ORACLE_USER", "DEBEZIUM");
    String pass = System.getenv().getOrDefault("ORACLE_PASSWORD", "debezium_pw");

    System.out.println("Connecting to: " + url + " as " + user);

    try (Connection conn = DriverManager.getConnection(url, user, pass)) {
      DatabaseMetaData meta = conn.getMetaData();
      System.out.println("Connected. Product=" + meta.getDatabaseProductName());
      System.out.println("ProductVersion=" + meta.getDatabaseProductVersion());
      System.out.println("Major=" + meta.getDatabaseMajorVersion() + " Minor=" + meta.getDatabaseMinorVersion());

      runQuery(conn, "select sys_context('USERENV','CON_NAME') as con_name from dual");
      runQuery(conn, "select banner from v$version");

      // Debezium's version resolution logic (observed in 2.7.x and 3.0.0.Final)
      runQuery(conn, "select banner_full from v$version where banner_full like 'Oracle Database%'");
      runQuery(conn, "select banner from v$version where banner like 'Oracle Database%'");

      if (hasArg(args, "--apply-workaround")) {
        // Creates schema-local views so Debezium's LIKE 'Oracle Database%' query returns a row.
        // Requires CREATE VIEW privilege for this user.
        String debeziumBanner = "Oracle Database 23c Free Release 23.26.0.0.0 - Debezium compatibility banner";
        runStatement(
            conn,
          "CREATE OR REPLACE VIEW V$VERSION AS SELECT '" + debeziumBanner + "' AS BANNER, '" + debeziumBanner
            + "' AS BANNER_FULL FROM dual");
        runStatement(
            conn,
          "CREATE OR REPLACE VIEW V_$VERSION AS SELECT '" + debeziumBanner + "' AS BANNER, '" + debeziumBanner
            + "' AS BANNER_FULL FROM dual");

        // Re-run the Debezium queries after applying.
        runQuery(conn, "select banner_full from v$version where banner_full like 'Oracle Database%'");
        runQuery(conn, "select banner from v$version where banner like 'Oracle Database%'");
      }

      runQuery(conn, "select version from product_component_version where product like 'Oracle Database%'");
      runQuery(conn, "select instance_name, version from v$instance");
    }
  }
}
