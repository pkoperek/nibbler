import org.apache.spark.{SparkContext, SparkConf}

class SparkContextService(nibblerJarRealPath: String) {

    private val executorUri = "http://d3kbcqa49mib13.cloudfront.net/spark-1.0.0-bin-hadoop2.tgz"


    def createSparkContext(appName: String, masterUrl: String): SparkContext = {
        val conf = new SparkConf()
                    .setAppName(appName)
                    .set("spark.executor.uri", executorUri)
                    .setMaster(masterUrl)
                    .setSparkHome("someHomeWhichShouldBeIrrelevant")

        val ctx = new SparkContext(conf)        
        ctx.addJar(nibblerJarRealPath)
        ctx
    }    
}
