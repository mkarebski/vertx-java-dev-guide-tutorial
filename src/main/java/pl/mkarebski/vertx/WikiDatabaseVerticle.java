package pl.mkarebski.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class WikiDatabaseVerticle extends AbstractVerticle {

  public enum ErrorCodes {
    NO_ACTION_SPECIFIED,
    BAD_ACTION,
    DB_ERROR
  }

  public static final String CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url";
  public static final String CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class";
  public static final String CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size";
  public static final String CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file";
  public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

  private static final Logger log = LoggerFactory.getLogger(WikiDatabaseVerticle.class);

  private JDBCClient dbClient;

  private enum SqlQuery {
    CREATE_PAGES_TABLE,
    ALL_PAGES,
    GET_PAGE,
    CREATE_PAGE,
    SAVE_PAGE,
    DELETE_PAGE
  }

  private final HashMap<SqlQuery, String> sqlQueries = new HashMap<>();

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    loadSqlQueries();

    String jdbcUrl = config().getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:file:db/wiki");
    String jdbcDriver = config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver");
    int maxPoolSize = config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 30);

    dbClient = getDBClient(jdbcUrl, jdbcDriver, maxPoolSize);

    dbClient.getConnection(asyncResult -> {
      if (asyncResult.failed()) {
        log.error("Could not open a database connection", asyncResult.cause());
        startFuture.fail(asyncResult.cause());
      } else {
        SQLConnection connection = asyncResult.result();
        connection.execute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE), create -> {
          connection.close();
          if (create.failed()) {
            log.error("Database preparation error", create.cause());
            startFuture.fail(asyncResult.cause());
          } else {
            log.info("Database created");
            vertx.eventBus().consumer(config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue"), this::onMessage);
            startFuture.complete();
          }
        });
      }
    });
  }

  @Override
  public void stop(Future<Void> stopFuture) throws Exception {
    super.stop(stopFuture);
  }

  private void loadSqlQueries() throws IOException {
    String queriesFile = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE);
    InputStream queriesInputStream;
    if (queriesFile != null) {
      queriesInputStream = new FileInputStream(queriesFile);
    } else {
      queriesInputStream = getClass().getResourceAsStream("/db-queries.properties");
    }

    Properties queriesProps = new Properties();
    queriesProps.load(queriesInputStream);
    queriesInputStream.close();

    sqlQueries.put(SqlQuery.CREATE_PAGES_TABLE, queriesProps.getProperty("sql.create.db"));
    sqlQueries.put(SqlQuery.ALL_PAGES, queriesProps.getProperty("sql.page.get.all"));
    sqlQueries.put(SqlQuery.GET_PAGE, queriesProps.getProperty("sql.page.get.one"));
    sqlQueries.put(SqlQuery.CREATE_PAGE, queriesProps.getProperty("sql.page.create"));
    sqlQueries.put(SqlQuery.SAVE_PAGE, queriesProps.getProperty("sql.page.update"));
    sqlQueries.put(SqlQuery.DELETE_PAGE, queriesProps.getProperty("sql.page.delete"));
  }

  private JDBCClient getDBClient(final String jdbcUrl, final String driver, final int maxPoolSize) {
    return JDBCClient.createShared(vertx, new JsonObject()
      .put("url", jdbcUrl)
      .put("driver_class", driver)
      .put("max_pool_size", maxPoolSize));
  }

  public void onMessage(Message<JsonObject> message) {
    if (!message.headers().contains("action")) {
      message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header specified");
    }
    String action = message.headers().get("action");

    switch (action) {
      case "all-pages":
        fetchAllPages(message);
        break;
      case "get-page":
        fetchPage(message);
        break;
      case "create-page":
        createPage(message);
        break;
      case "update-page":
        updatePage(message);
        break;
      case "delete-page":
        deletePage(message);
        break;
      default:
        message.fail(ErrorCodes.BAD_ACTION.ordinal(), "Bad action: " + action);
    }
  }

  private void fetchAllPages(Message<JsonObject> message) {
    dbClient.getConnection(asyncResult -> {
      if (asyncResult.succeeded()) {
        SQLConnection sqlConnection = asyncResult.result();
        sqlConnection.query(sqlQueries.get(SqlQuery.ALL_PAGES), result -> {
          sqlConnection.close();
          if (result.succeeded()) {
            List<String> pages = result.result().getResults().stream().map(json -> json.getString(0)).sorted().collect(Collectors.toList());
            message.reply(new JsonObject().put("pages", new JsonArray(pages)));
          } else {
            reportQueryError(message, result.cause());
          }
        });
      } else {
        reportQueryError(message, asyncResult.cause());
      }
    });
  }

  private void fetchPage(Message<JsonObject> message) {
    String requestedPage = message.body().getString("page");

    dbClient.getConnection(asyncResult -> {
      if (asyncResult.succeeded()) {
        SQLConnection connection = asyncResult.result();
        connection.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), new JsonArray().add(requestedPage), result -> {
          connection.close();
          if (result.succeeded()) {
            JsonObject response = new JsonObject();
            ResultSet resultSet = result.result();
            if (resultSet.getNumRows() == 0) {
              response.put("found", false);
            } else {
              response.put("found", true);
              JsonArray row = resultSet.getResults().get(0);
              response.put("id", row.getInteger(0));
              response.put("rawContent", row.getString(1));
            }
            message.reply(response);
          } else {
            reportQueryError(message, result.cause());
          }
        });
      } else {
        reportQueryError(message, asyncResult.cause());
      }
    });
  }

  private void createPage(Message<JsonObject> message) {
    JsonObject request = message.body();

    String title = request.getString("title");
    String rawContent = request.getString("markdown");

    dbClient.getConnection(asyncResult -> {
      if (asyncResult.succeeded()) {
        SQLConnection connection = asyncResult.result();
        connection.updateWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), new JsonArray().add(title).add(rawContent), result -> {
          connection.close();
          if (result.succeeded()) {
            message.reply("ok");
          } else {
            reportQueryError(message, result.cause());
          }
        });
      } else {
        reportQueryError(message, asyncResult.cause());
      }
    });
  }

  private void updatePage(Message<JsonObject> message) {
    JsonObject request = message.body();

    String id = request.getString("id");
    String title = request.getString("title");
    String rawContent = request.getString("markdown");

    dbClient.getConnection(asyncResult -> {
      if (asyncResult.succeeded()) {
        SQLConnection connection = asyncResult.result();
        connection.updateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), new JsonArray().add(id).add(title).add(rawContent), result -> {
          connection.close();
          if (result.succeeded()) {
            message.reply("ok");
          } else {
            reportQueryError(message, result.cause());
          }
        });
      } else {
        reportQueryError(message, asyncResult.cause());
      }
    });
  }

  private void deletePage(Message<JsonObject> message) {
    JsonObject request = message.body();

    int id = request.getInteger("id");

    dbClient.getConnection(asyncResult -> {
      if (asyncResult.succeeded()) {
        SQLConnection connection = asyncResult.result();
        connection.updateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), new JsonArray().add(id), result -> {
          if (result.succeeded()) {
            message.reply("ok");
          } else {
            reportQueryError(message, result.cause());
          }
        });
      } else {
        reportQueryError(message, asyncResult.cause());
      }
    });
  }

  private void reportQueryError(Message<JsonObject> message, Throwable cause) {
    log.error("Database query error", cause);
    message.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
  }
}
