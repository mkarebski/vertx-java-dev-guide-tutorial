package pl.mkarebski.vertx.database;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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

import static pl.mkarebski.vertx.database.ErrorCodes.*;
import static pl.mkarebski.vertx.database.SqlQuery.*;
import static pl.mkarebski.vertx.database.SqlQuery.ALL_PAGES;
import static pl.mkarebski.vertx.database.SqlQuery.CREATE_PAGES_TABLE;
import static pl.mkarebski.vertx.database.SqlQuery.GET_PAGE;

class WikiDatabaseServiceImpl implements WikiDatabaseService {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseServiceImpl.class);

  private final HashMap<SqlQuery, String> sqlQueries;
  private final JDBCClient dbClient;

  WikiDatabaseServiceImpl(JDBCClient dbClient, HashMap<SqlQuery, String> sqlQueries, Handler<AsyncResult<WikiDatabaseService>> readyHandler) {
    this.dbClient = dbClient;
    this.sqlQueries = sqlQueries;

    dbClient.getConnection(asyncResult -> {
      if (asyncResult.failed()) {
        LOGGER.error("Could not open a database connection", asyncResult.cause());
        readyHandler.handle(Future.failedFuture(asyncResult.cause()));
      } else {
        SQLConnection connection = asyncResult.result();
        connection.execute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE), create -> {
          connection.close();
          if (create.failed()) {
            LOGGER.error("Database preparation error", create.cause());
            readyHandler.handle(Future.failedFuture(create.cause()));
          } else {
            readyHandler.handle(Future.succeededFuture(this));
          }
        });
      }
    });
  }

  @Override
  public WikiDatabaseService fetchAllPages(Handler<AsyncResult<JsonArray>> resultHandler) {
    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection connection = car.result();
        connection.query(sqlQueries.get(SqlQuery.ALL_PAGES), res -> {
          connection.close();
          if (res.succeeded()) {
            JsonArray pages = new JsonArray(res.result()
              .getResults()
              .stream()
              .map(json -> json.getString(0))
              .sorted()
              .collect(Collectors.toList()));
            resultHandler.handle(Future.succeededFuture(pages));
          } else {
            LOGGER.error("Database query error", res.cause());
            resultHandler.handle(Future.failedFuture(res.cause()));
          }
        });
      } else {
        LOGGER.error("Database query error", car.cause());
        resultHandler.handle(Future.failedFuture(car.cause()));
      }
    });
    return this;
  }

  @Override
  public WikiDatabaseService fetchPage(String name, Handler<AsyncResult<JsonObject>> resultHandler) {
    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection connection = car.result();
        connection.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), new JsonArray().add(name), fetch -> {
          connection.close();
          if (fetch.succeeded()) {
            JsonObject response = new JsonObject();
            ResultSet resultSet = fetch.result();
            if (resultSet.getNumRows() == 0) {
              response.put("found", false);
            } else {
              response.put("found", true);
              JsonArray row = resultSet.getResults().get(0);
              response.put("id", row.getInteger(0));
              response.put("rawContent", row.getString(1));
            }
            resultHandler.handle(Future.succeededFuture(response));
          } else {
            LOGGER.error("Database query error", fetch.cause());
            resultHandler.handle(Future.failedFuture(fetch.cause()));
          }
        });
      } else {
        LOGGER.error("Database query error", car.cause());
        resultHandler.handle(Future.failedFuture(car.cause()));
      }
    });
    return this;
  }

  @Override
  public WikiDatabaseService createPage(String title, String markdown, Handler<AsyncResult<Void>> resultHandler) {
    dbClient.getConnection(car -> {

      if (car.succeeded()) {
        SQLConnection connection = car.result();
        JsonArray data = new JsonArray().add(title).add(markdown);
        connection.updateWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), data, res -> {
          connection.close();
          if (res.succeeded()) {
            resultHandler.handle(Future.succeededFuture());
          } else {
            LOGGER.error("Database query error", res.cause());
            resultHandler.handle(Future.failedFuture(res.cause()));
          }
        });
      } else {
        LOGGER.error("Database query error", car.cause());
        resultHandler.handle(Future.failedFuture(car.cause()));
      }
    });
    return this;
  }

  @Override
  public WikiDatabaseService savePage(int id, String markdown, Handler<AsyncResult<Void>> resultHandler) {
    dbClient.getConnection(car -> {

      if (car.succeeded()) {
        SQLConnection connection = car.result();
        JsonArray data = new JsonArray().add(markdown).add(id);
        connection.updateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), data, res -> {
          connection.close();
          if (res.succeeded()) {
            resultHandler.handle(Future.succeededFuture());
          } else {
            LOGGER.error("Database query error", res.cause());
            resultHandler.handle(Future.failedFuture(res.cause()));
          }
        });
      } else {
        LOGGER.error("Database query error", car.cause());
        resultHandler.handle(Future.failedFuture(car.cause()));
      }
    });
    return this;
  }

  @Override
  public WikiDatabaseService deletePage(int id, Handler<AsyncResult<Void>> resultHandler) {
    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection connection = car.result();
        JsonArray data = new JsonArray().add(id);
        connection.updateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data, res -> {
          connection.close();
          if (res.succeeded()) {
            resultHandler.handle(Future.succeededFuture());
          } else {
            LOGGER.error("Database query error", res.cause());
            resultHandler.handle(Future.failedFuture(res.cause()));
          }
        });
      } else {
        LOGGER.error("Database query error", car.cause());
        resultHandler.handle(Future.failedFuture(car.cause()));
      }
    });
    return this;
  }
}
