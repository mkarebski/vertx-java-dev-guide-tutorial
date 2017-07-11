package pl.mkarebski.vertx;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;

import java.util.Date;

public class WikiHttpServerVerticle extends AbstractVerticle {

  public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";
  public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";

  private static final Logger log = LoggerFactory.getLogger(WikiHttpServerVerticle.class);

  private static final String EMPTY_PAGE_MARKDOWN =
    "# A new page\n" +
      "\n" +
      "Feel-free to write in Markdown!\n";

  private final FreeMarkerTemplateEngine templateEngine = FreeMarkerTemplateEngine.create();


  private String wikiDbQueue = "wikidb.queue";

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    wikiDbQueue = config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue");

    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);
    router.get("/wiki/:page").handler(this::pageRenderingHandler);
    router.post().handler(BodyHandler.create());
    router.post("/save").handler(this::pageUpdateHandler);
    router.post("/create").handler(this::pageCreateHandler);
    router.post("/delete").handler(this::pageDeletionHandler);

    int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
    server
      .requestHandler(router::accept)
      .listen(portNumber, asyncResult -> {
        if (asyncResult.succeeded()) {
          log.info("HTTP server running on port " + portNumber);
          startFuture.complete();
        } else {
          log.error("Could not start a HTTP server", asyncResult.cause());
          startFuture.fail(asyncResult.cause());
        }
      });
  }

  @Override
  public void stop(Future<Void> stopFuture) throws Exception {
    super.stop(stopFuture);
  }

  private void indexHandler(RoutingContext context) {
    DeliveryOptions options = new DeliveryOptions().addHeader("action", "all-pages");

    vertx.eventBus().send(wikiDbQueue, new JsonObject(), options, reply -> {
      if (reply.succeeded()) {
        JsonObject body = (JsonObject) reply.result().body();
        context.put("title", "Wiki home");
        context.put("pages", body.getJsonArray("pages").getList());
        templateEngine.render(context, "templates/index.ftl", ar -> {
          if (ar.succeeded()) {
            context.response().putHeader("Content-Type", "text/html");
            context.response().end(ar.result());
          } else {
            context.fail(ar.cause());
          }
        });
      } else {
        context.fail(reply.cause());
      }
    });
  }

  private void pageRenderingHandler(RoutingContext context) {
    DeliveryOptions options = new DeliveryOptions().addHeader("action", "get-page");

    String requestedPage = context.request().getParam("page");

    JsonObject request = new JsonObject().put("page", requestedPage);

    vertx.eventBus().send(wikiDbQueue, request, options, reply -> {
      if (reply.succeeded()) {
        JsonObject body = (JsonObject) reply.result().body();

        int id = body.getInteger("id", -1);
        boolean found = body.getBoolean("found");
        String rawContent = body.getString("rawContent", EMPTY_PAGE_MARKDOWN);

        context.put("title", requestedPage);
        context.put("id", id);
        context.put("newPage", found ? "yes" : "no");
        context.put("rawContent", rawContent);
        context.put("content", Processor.process(rawContent));
        context.put("timestamp", new Date().toString());

        templateEngine.render(context, "templates/page.ftl", ar -> {
          if (ar.succeeded()) {
            context.response().putHeader("Content-Type", "text/html");
            context.response().end(ar.result());
          } else {
            context.fail(ar.cause());
          }
        });
      } else {
        context.fail(reply.cause());
      }
    });
  }

  private void pageCreateHandler(RoutingContext context) {
    String pageName = context.request().getParam("name");
    String location = "/wiki/" + pageName;
    if (pageName == null || pageName.isEmpty()) {
      location = "/";
    }
    context.response().setStatusCode(303);
    context.response().putHeader("Location", location);
    context.response().end();
  }

  private void pageUpdateHandler(RoutingContext context) {
    DeliveryOptions options = new DeliveryOptions();

    String id = context.request().getParam("id");
    String title = context.request().getParam("title");
    String markdown = context.request().getParam("markdown");

    JsonObject request = new JsonObject()
      .put("id", id)
      .put("title", title)
      .put("markdown", markdown);

    boolean newPage = "yes".equals(context.request().getParam("newPage"));
    if (newPage) {
      options.addHeader("action", "create-page");
    } else {
      options.addHeader("action", "update-page");
    }

    vertx.eventBus().send(wikiDbQueue, request, options, reply -> {
      if (reply.succeeded()) {
        context.response().setStatusCode(303);
        context.response().putHeader("Location", "/wiki/" + title);
        context.response().end();
      } else {
        context.fail(reply.cause());
      }
    });
  }

  private void pageDeletionHandler(RoutingContext context) {
    String id = context.request().getParam("id");
    DeliveryOptions options = new DeliveryOptions().addHeader("action", "delete-page");
    JsonObject request = new JsonObject().put("id", id);

    vertx.eventBus().send(wikiDbQueue, request, options, reply -> {
      if (reply.succeeded()) {
        context.response().setStatusCode(303);
        context.response().putHeader("Location", "/");
        context.response().end();
      } else {
        context.fail(reply.cause());
      }
    });
  }
}
