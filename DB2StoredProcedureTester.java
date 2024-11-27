import java.sql.*;
import java.util.*;
import java.io.*;
import org.yaml.snakeyaml.Yaml;

public class DB2StoredProcedureTester {
    private static Connection connectToDB2(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    private static Map<String, Object> loadYaml(String filePath) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        try (InputStream inputStream = new FileInputStream(filePath)) {
            return yaml.load(inputStream);
        }
    }

    private static boolean callAndValidateStoredProcedure(Connection conn, Map<String, Object> config) throws SQLException {
        String storedProcedure = (String) config.get("stored_procedure");
        List<Map<String, String>> params = (List<Map<String, String>>) config.get("params");
        List<Map<String, String>> expectedOutput = (List<Map<String, String>>) config.get("expected_output");

        // Build the callable statement
        StringBuilder procedureCall = new StringBuilder("{call ").append(storedProcedure).append("(");
        for (int i = 0; i < params.size(); i++) {
            procedureCall.append("?");
            if (i < params.size() - 1) procedureCall.append(", ");
        }
        procedureCall.append(")}");

        try (CallableStatement stmt = conn.prepareCall(procedureCall.toString())) {
            // Set input parameters
            int paramIndex = 1;
            for (Map<String, String> param : params) {
                stmt.setString(paramIndex++, param.values().iterator().next());
            }

            // Execute the stored procedure
            ResultSet rs = stmt.executeQuery();

            // Validate output
            boolean isValid = true;
            if (rs.next()) {
                for (Map<String, String> expected : expectedOutput) {
                    for (Map.Entry<String, String> entry : expected.entrySet()) {
                        String columnName = entry.getKey();
                        String expectedValue = entry.getValue();
                        String actualValue = rs.getString(columnName);
                        if (!expectedValue.equals(actualValue)) {
                            System.out.println("Validation failed for column: " + columnName);
                            isValid = false;
                        }
                    }
                }
            }

            return isValid;
        }
    }

    private static void runTeardownQuery(Connection conn, String teardownQuery) throws SQLException {
        if (teardownQuery != null && !teardownQuery.isEmpty()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(teardownQuery);
            }
        }
    }

    public static void main(String[] args) {
        String dbUrl = "jdbc:db2://your-db-host:50000/your-database";
        String user = "your-username";
        String password = "your-password";
        String yamlFilePath = "test-config.yaml";

        try (Connection conn = connectToDB2(dbUrl, user, password)) {
            // Load YAML configuration
            Map<String, Object> config = loadYaml(yamlFilePath);

            // Call stored procedure and validate output
            boolean result = callAndValidateStoredProcedure(conn, config);

            // Run teardown query if defined
            String teardownQuery = (String) config.get("teardown_query");
            runTeardownQuery(conn, teardownQuery);

            // Print result
            System.out.println("Test " + (result ? "PASSED" : "FAILED"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
