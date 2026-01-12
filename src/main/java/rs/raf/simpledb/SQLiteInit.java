// java
package rs.raf.simpledb;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SQLiteInit {

    private static final String DB_FILE = "raf.db";
    private static final String RESOURCES_DIR = "src/main/resources";

    public static void main(String[] args) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_FILE)) {
            conn.setAutoCommit(false);

            importCsv(conn, Path.of(RESOURCES_DIR, "smer.csv").toString(), "SMER");
            importCsv(conn, Path.of(RESOURCES_DIR, "studenti.csv").toString(), "STUDENT");
            importCsv(conn, Path.of(RESOURCES_DIR, "raf_predmeti.csv").toString(), "PREDMET");
            importCsv(conn, Path.of(RESOURCES_DIR, "ispitni_rok.csv").toString(), "ISPITNIROK");
            importCsv(conn, Path.of(RESOURCES_DIR, "ispiti.csv").toString(), "ISPIT");
            importCsv(conn, Path.of(RESOURCES_DIR, "polaganja.csv").toString(), "POLAGANJE");
            // add other CSVs similarly (e.g., ocene.csv) if present

            conn.commit();
            System.out.println("Import finished. DB: " + DB_FILE);
        }
    }

    private static void importCsv(Connection conn, String csvPath, String tableName) throws IOException, SQLException, CsvValidationException {
        try (CSVReader reader = new CSVReader(new FileReader(csvPath))) {
            String[] header = reader.readNext();
            if (header == null) {
                System.out.println("Empty CSV: " + csvPath);
                return;
            }

            // sanitize and use header names as columns
            String[] cols = new String[header.length];
            for (int i = 0; i < header.length; i++) {
                cols[i] = header[i].trim().replaceAll("[^a-zA-Z0-9_]", "_");
            }

            // read first N rows to infer integer columns (simple heuristic)
            List<String[]> sample = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                String[] row = reader.readNext();
                if (row == null) break;
                sample.add(row);
            }

            boolean[] isInteger = new boolean[cols.length];
            for (int c = 0; c < cols.length; c++) isInteger[c] = true;
            for (String[] r : sample) {
                for (int c = 0; c < cols.length && c < r.length; c++) {
                    String v = r[c].trim();
                    if (v.isEmpty()) continue;
                    try { Integer.parseInt(v); } catch (NumberFormatException ex) { isInteger[c] = false; }
                }
            }

            // create table statement
            StringBuilder create = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName).append(";\nCREATE TABLE ").append(tableName).append(" (");
            for (int i = 0; i < cols.length; i++) {
                create.append(cols[i]).append(' ').append(isInteger[i] ? "INTEGER" : "TEXT");
                if (i < cols.length - 1) create.append(", ");
            }
            create.append(");");
            try (Statement st = conn.createStatement()) {
                st.executeUpdate(create.toString());
            }

            // prepare insert
            StringBuilder placeholders = new StringBuilder();
            for (int i = 0; i < cols.length; i++) placeholders.append("?");
            String ph = placeholders.toString().replaceAll("", ", ").trim();
            ph = ph.substring(1, ph.length() - 1); // build ",?,?..." then trim
            String insertSql = "INSERT INTO " + tableName + " (" + String.join(",", cols) + ") VALUES (" +
                    String.join(",", java.util.Collections.nCopies(cols.length, "?")) + ")";
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                // insert sampled rows first
                for (String[] row : sample) {
                    bindRow(ps, row, cols.length);
                    ps.addBatch();
                }

                // continue reading remaining rows
                String[] row;
                while ((row = reader.readNext()) != null) {
                    bindRow(ps, row, cols.length);
                    ps.addBatch();
                }

                ps.executeBatch();
            }

            System.out.println("Imported " + tableName + " from " + csvPath);
        }
    }

    private static void bindRow(PreparedStatement ps, String[] row, int colCount) throws SQLException {
        for (int i = 0; i < colCount; i++) {
            String val = i < row.length ? row[i] : null;
            if (val == null || val.isEmpty()) {
                ps.setNull(i + 1, Types.NULL);
            } else {
                // try integer
                try {
                    int v = Integer.parseInt(val.trim());
                    ps.setInt(i + 1, v);
                } catch (NumberFormatException ex) {
                    ps.setString(i + 1, val.trim());
                }
            }
        }
    }
}
