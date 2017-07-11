package pl.mkarebski.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class MainVerticle extends AbstractVerticle {

  private static final Logger log = LoggerFactory.getLogger(MainVerticle.class);

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    /*Future<String> dbVerticleDeployment = Future.future();
    vertx.deploy(
      new WikiDatabaseVerticle(),
      dbVerticleDeployment.completer());
*/

    /*Future<String> httpVerticleDeployment = Future.future();
    vertx.deploy(
      "pl.mkarebski.vertx.WikiHttpServerVerticle",
      new DeploymentOptions().setInstances(2),
      httpVerticleDeployment.completer());
*/
    String dbVerticle = WikiDatabaseVerticle.class.getCanonicalName();
    String httpVerticle = WikiHttpServerVerticle.class.getCanonicalName();

    deploy(dbVerticle, 1)
        .compose(event -> deploy(httpVerticle, 2))
        .setHandler(verticleId -> startFuture.completer());

  }

  @Override
  public void stop(Future<Void> stopFuture) throws Exception {
    super.stop(stopFuture);
  }

  private Future<String> deploy(String className, int instanceNumber) {
    Future<String> verticleDeployment = Future.future();
    vertx.deployVerticle(
      className,
      new DeploymentOptions().setInstances(instanceNumber),
      verticleDeployment.completer()
    );
    return verticleDeployment;
  }

}
