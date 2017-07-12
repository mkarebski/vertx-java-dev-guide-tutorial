package pl.mkarebski.vertx.database;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@RunWith(VertxUnitRunner.class)
public class DatabaseCrudTest {

  private Vertx vertx;
  private WikiDatabaseService dbService;

  @Before
  public void prepare(TestContext context) throws InterruptedException {
    vertx = Vertx.vertx();

    JsonObject conf = new JsonObject()
      .put(WikiDatabaseVerticle.CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:mem:testdb;shutdown=true")
      .put(WikiDatabaseVerticle.CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 4);

    vertx.deployVerticle(
      new WikiDatabaseVerticle(),
      new DeploymentOptions().setConfig(conf),
      context.asyncAssertSuccess(id ->
        dbService = WikiDatabaseService.createProxy(vertx, WikiDatabaseVerticle.CONFIG_WIKIDB_QUEUE)
      )
    );
  }

  @Test
  public void shouldPassWithHappyPath(TestContext context) {
    Async async = context.async();

    dbService.createPage("Test", "Some content", context.asyncAssertSuccess(createResult -> {

      dbService.fetchPage("Test", context.asyncAssertSuccess(fetchPageResult -> {
        assertThat(fetchPageResult, is(notNullValue()));
        assertThat(fetchPageResult.getBoolean("found"), is(true));
        assertThat(fetchPageResult.containsKey("id"), is(true));
        assertThat(fetchPageResult.getString("rawContent"), is("Some content"));

        dbService.savePage(fetchPageResult.getInteger("id"), "Yo!", context.asyncAssertSuccess(updateResult -> {

          dbService.fetchAllPages(context.asyncAssertSuccess(allPages -> {
            assertThat(allPages, is(notNullValue()));
            assertThat(allPages.size(), is(1));

            dbService.fetchPage("Test", context.asyncAssertSuccess(getPageResult -> {
              assertThat(getPageResult, is(notNullValue()));
              assertThat(getPageResult.getString("rawContent"), is("Yo!"));

              dbService.deletePage(fetchPageResult.getInteger("id"), deletePage -> {
                  assertThat(deletePage, is(notNullValue()));

                dbService.fetchAllPages(context.asyncAssertSuccess(allPages2 -> {
                  assertThat(allPages2, is(notNullValue()));
                  assertThat(allPages2.isEmpty(), is(true));
                  async.complete();
                }));
              });
            }));
          }));
        }));
      }));
    }));
    async.awaitSuccess(2000);
  }

  @After
  public void finish(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

}
