package com.lit.fire.flame;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class App {

    private static final int BATCH_SIZE = 1000;
    // This map acts as a cache of entity_ids by keyword. A single keyword may map to
    // multiple entities (e.g. a newly added entity that shares an existing keyword), so a
    // post matching that keyword must be linked to every one of them via mention_entities.
    private static final Map<String, List<Long>> KEYWORD_TO_IDS_MAP = new HashMap<>();

    public static void main(String[] args) {
        Properties props = loadProperties();
        String url = props.getProperty("db.url");
        String user = props.getProperty("db.user");
        String password = props.getProperty("db.password");

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            connection.setAutoCommit(false);

            // Pre-load the keyword map from the entity_keywords table
            loadEntityKeywords(connection);
            System.out.println("Loaded " + KEYWORD_TO_IDS_MAP.size() + " entity keywords.");

            transferData(connection, "instagram_posts", "Instagram", "id", "text", "timestamp", "author", "sentiment_score", "keyword", "permalink", "sentiment_category");
            transferData(connection, "x_posts", "X", "id", "text", "created_at", "author", "sentiment_score", "keyword", "permalink", "sentiment_category");
            transferData(connection, "youtube_comments", "YouTube", "id", "text", "published_at", "author", "sentiment_score", "keyword", "permalink", "sentiment_category");
            transferData(connection, "reddit_posts", "Reddit", "id", "text", "created_at", "author", "sentiment_score", "keyword", "permalink", "sentiment_category");

            System.out.println("Data consolidation complete.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void loadEntityKeywords(Connection connection) throws SQLException {
        String sql = "SELECT entity_id, keyword FROM entity_keywords";
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery(sql)) {

            rs.last();
            int rowCount = rs.getRow();
            rs.beforeFirst();
            
            System.out.println("Found " + rowCount + " rows in entity_keywords table.");

            while (rs.next()) {
                String keyword = rs.getString("keyword");
                long entityId = rs.getLong("entity_id");
                if (keyword != null) {
                    KEYWORD_TO_IDS_MAP.computeIfAbsent(keyword.toLowerCase(), k -> new ArrayList<>()).add(entityId);
                }
            }
        }
    }

    private static List<Long> getEntityIds(String keyword) {
        if (keyword == null || keyword.trim().isEmpty()) {
            return Collections.emptyList(); // No keyword — nothing to link
        }
        return KEYWORD_TO_IDS_MAP.getOrDefault(keyword.toLowerCase(), Collections.emptyList());
    }

    private static void transferData(Connection connection, String sourceTable, String platform,
                                     String idColumn, String textColumn, String dateColumn,
                                     String authorColumn, String sentimentScoreColumn, String keywordColumn, String permalinkColumn, String sentimentCategoryColumn) throws SQLException {
        String selectSql = "SELECT " + idColumn + ", " + textColumn + ", " + dateColumn + ", " +
                           (authorColumn != null ? authorColumn + ", " : "") + sentimentScoreColumn + ", " + keywordColumn + ", " + permalinkColumn + ", " + sentimentCategoryColumn +
                           " FROM " + sourceTable;

        // Upsert the mention itself. A mention is UNIQUE on post_id, so the same post matching
        // a second entity must reuse this row rather than insert a duplicate. RETURNING id gives
        // us the mention's id on both insert and conflict-update, which we then link to entities.
        String upsertSql = "INSERT INTO mentions (platform, post_id, content, post_date, author, sentiment, sentiment_score, permalink, sentiment_category) " +
                           "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (post_id) DO UPDATE SET " +
                           "sentiment = EXCLUDED.sentiment, " +
                           "sentiment_score = EXCLUDED.sentiment_score, " +
                           "author = COALESCE(mentions.author, EXCLUDED.author), " +
                           "permalink = CASE WHEN mentions.permalink IS NULL AND EXCLUDED.permalink IS NOT NULL THEN EXCLUDED.permalink ELSE mentions.permalink END, " +
                           "sentiment_category = EXCLUDED.sentiment_category " +
                           "RETURNING id";

        // Link the mention to every entity whose keyword it matched. The join table's inverse column
        // is managed_entity_id (see AuraService Mention#managedEntities). ON CONFLICT DO NOTHING keeps
        // this idempotent across re-runs and when the same post matches multiple entities.
        String linkSql = "INSERT INTO mention_entities (mention_id, managed_entity_id) VALUES (?, ?) ON CONFLICT (mention_id, managed_entity_id) DO NOTHING";

        // For irrelevant posts, drop the entity links first (FK), then the mention itself.
        String deleteLinksSql = "DELETE FROM mention_entities WHERE mention_id IN (SELECT id FROM mentions WHERE post_id = ?)";
        String deleteSql = "DELETE FROM mentions WHERE post_id = ?";

        int count = 0;
        int linkCount = 0;
        int deleteCount = 0;
        try (PreparedStatement selectStmt = connection.prepareStatement(selectSql);
             ResultSet rs = selectStmt.executeQuery();
             PreparedStatement upsertStmt = connection.prepareStatement(upsertSql);
             PreparedStatement linkStmt = connection.prepareStatement(linkSql);
             PreparedStatement deleteLinksStmt = connection.prepareStatement(deleteLinksSql);
             PreparedStatement deleteStmt = connection.prepareStatement(deleteSql)) {

            while (rs.next()) {
                String keyword = rs.getString(keywordColumn);

                List<Long> entityIds = getEntityIds(keyword);

                if (entityIds.isEmpty()) { // Skip if keyword matched no entity
                    continue;
                }

                String postId = rs.getString(idColumn);

                int sentimentScore = rs.getInt(sentimentScoreColumn);
                if (sentimentScore <= 0) { // Not relevant to the user — remove any existing row from the destination
                    deleteLinksStmt.setString(1, postId);
                    deleteLinksStmt.addBatch();
                    deleteStmt.setString(1, postId);
                    deleteStmt.addBatch();
                    if (++deleteCount % BATCH_SIZE == 0) {
                        deleteLinksStmt.executeBatch();
                        deleteStmt.executeBatch();
                        connection.commit();
                    }
                    continue;
                }

                String sentiment;
                if (sentimentScore > 75) {
                    sentiment = "POSITIVE";
                } else if (sentimentScore <= 50) {
                    sentiment = "NEGATIVE";
                } else {
                    sentiment = "NEUTRAL";
                }
                String sentimentCategory = rs.getString(sentimentCategoryColumn);

                upsertStmt.setString(1, platform.toUpperCase());
                upsertStmt.setString(2, postId);
                upsertStmt.setString(3, rs.getString(textColumn));
                upsertStmt.setTimestamp(4, rs.getTimestamp(dateColumn));
                upsertStmt.setString(5, authorColumn != null ? rs.getString(authorColumn) : null);
                upsertStmt.setString(6, sentiment);
                upsertStmt.setInt(7, sentimentScore);
                upsertStmt.setString(8, rs.getString(permalinkColumn));
                upsertStmt.setString(9, sentimentCategory);

                long mentionId;
                try (ResultSet keyRs = upsertStmt.executeQuery()) {
                    keyRs.next();
                    mentionId = keyRs.getLong(1);
                }

                // Attach this mention to every entity that shares the matched keyword.
                for (Long entityId : entityIds) {
                    linkStmt.setLong(1, mentionId);
                    linkStmt.setLong(2, entityId);
                    linkStmt.addBatch();
                    linkCount++;
                }

                if (++count % BATCH_SIZE == 0) {
                    linkStmt.executeBatch();
                    connection.commit();
                }
            }
            linkStmt.executeBatch(); // Link remaining records
            deleteLinksStmt.executeBatch(); // Unlink remaining irrelevant records
            deleteStmt.executeBatch(); // Delete remaining irrelevant records
            connection.commit();
        }
        System.out.println("Transferred " + count + " posts (" + linkCount + " entity links) from " + sourceTable + " (removed " + deleteCount + " irrelevant rows)");
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
