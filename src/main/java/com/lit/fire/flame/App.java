package com.lit.fire.flame;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class App {

    private static final int BATCH_SIZE = 1000;
    // This map will act as a cache for entity_id by keyword.
    private static final Map<String, Long> KEYWORD_TO_ID_MAP = new HashMap<>();

    public static void main(String[] args) {
        Properties props = loadProperties();
        String url = props.getProperty("db.url");
        String user = props.getProperty("db.user");
        String password = props.getProperty("db.password");

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            connection.setAutoCommit(false);

            // Pre-load the keyword map from the entity_keywords table
            loadEntityKeywords(connection);
            System.out.println("Loaded " + KEYWORD_TO_ID_MAP.size() + " entity keywords.");

            transferData(connection, "instagram_posts", "Instagram", "id", "text", "timestamp", null, "sentiment_score", "keyword", "permalink");
            transferData(connection, "x_posts", "X", "id", "text", "created_at", null, "sentiment_score", "keyword", "permalink");
            transferData(connection, "youtube_comments", "YouTube", "id", "text", "published_at", "author", "sentiment_score", "keyword", "permalink");
            transferData(connection, "reddit_posts", "Reddit", "id", "text", "created_at", "author", "sentiment_score", "keyword", "permalink");

            System.out.println("Data consolidation complete.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void loadEntityKeywords(Connection connection) throws SQLException {
        String sql = "SELECT entity_id, keyword FROM entity_keywords";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String keyword = rs.getString("keyword");
                long entityId = rs.getLong("entity_id");
                if (keyword != null) {
                    KEYWORD_TO_ID_MAP.put(keyword.toLowerCase(), entityId);
                }
            }
        }
    }

    private static long getEntityId(String keyword) {
        if (keyword == null || keyword.trim().isEmpty()) {
            return -1; // Sentinel value to indicate skipping
        }
        String lowerKeyword = keyword.toLowerCase();
        return KEYWORD_TO_ID_MAP.getOrDefault(lowerKeyword, -1L);
    }

    private static void transferData(Connection connection, String sourceTable, String platform,
                                     String idColumn, String textColumn, String dateColumn,
                                     String authorColumn, String sentimentScoreColumn, String keywordColumn, String permalinkColumn) throws SQLException {
        String selectSql = "SELECT " + idColumn + ", " + textColumn + ", " + dateColumn + ", " +
                           (authorColumn != null ? authorColumn + ", " : "") + sentimentScoreColumn + ", " + keywordColumn + ", " + permalinkColumn +
                           " FROM " + sourceTable;

        String insertSql = "INSERT INTO mentions (managed_entity_id, platform, post_id, content, post_date, author, sentiment, sentiment_score, permalink) " +
                           "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (post_id) DO UPDATE SET permalink = EXCLUDED.permalink WHERE mentions.permalink IS NULL AND EXCLUDED.permalink IS NOT NULL";

        try (PreparedStatement selectStmt = connection.prepareStatement(selectSql);
             ResultSet rs = selectStmt.executeQuery();
             PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {

            int count = 0;
            while (rs.next()) {
                String keyword = rs.getString(keywordColumn);
                
                long managedEntityId = getEntityId(keyword);

                if (managedEntityId == -1) { // Skip if keyword was not found
                    continue;
                }

                int sentimentScore = rs.getInt(sentimentScoreColumn);
                String sentiment;
                if (sentimentScore > 80) {
                    sentiment = "POSITIVE";
                } else if (sentimentScore < 60) {
                    sentiment = "NEGATIVE";
                } else {
                    sentiment = "NEUTRAL";
                }

                insertStmt.setLong(1, managedEntityId);
                insertStmt.setString(2, platform.toUpperCase());
                insertStmt.setString(3, rs.getString(idColumn));
                insertStmt.setString(4, rs.getString(textColumn));
                insertStmt.setTimestamp(5, rs.getTimestamp(dateColumn));
                insertStmt.setString(6, authorColumn != null ? rs.getString(authorColumn) : null);
                insertStmt.setString(7, sentiment);
                insertStmt.setInt(8, sentimentScore);
                insertStmt.setString(9, rs.getString(permalinkColumn));
                insertStmt.addBatch();

                if (++count % BATCH_SIZE == 0) {
                    insertStmt.executeBatch();
                    connection.commit();
                }
            }
            insertStmt.executeBatch(); // Insert remaining records
            connection.commit();
        }
    }

    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = App.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find application.properties");
                return props;
            }
            props.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return props;
    }
}
