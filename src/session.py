"""Create a classic SparkSession. Prepare/cache all JARs BEFORE class."""
import os

def make_spark(config):
    import time
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'): time.tzset()
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    master=config['spark_master']
    b=(SparkSession.builder.appName('Lab3-Lakehouse-'+config['group_id']).master(master)
       .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
       .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
       .config('spark.sql.session.timeZone','UTC')
       .config('spark.sql.ansi.enabled','true')
       .config('spark.sql.adaptive.enabled','true')
       .config('spark.sql.shuffle.partitions',str(config.get('shuffle_partitions',8)))
       .config('spark.databricks.delta.schema.autoMerge.enabled','false')
       .config('spark.databricks.delta.snapshotPartitions','2')
       .config('spark.redaction.regex','(?i)secret|password|token|access[.]?key|credential')
       .config('spark.ui.enabled','true'))
    if config.get('event_log_dir'):
        b=b.config('spark.eventLog.enabled','true').config('spark.eventLog.dir',config['event_log_dir'])
    if not master.startswith('local'):
        b=(b.config('spark.cores.max','4').config('spark.executor.cores','2')
             .config('spark.executor.memory','2g').config('spark.executor.instances','2'))
    if config['lakehouse_root'].startswith('s3a://'):
        ep=config['s3_endpoint']
        if not ep:raise RuntimeError('S3_ENDPOINT_REQUIRED')
        b=(b.config('spark.hadoop.fs.s3a.endpoint',ep)
           .config('spark.hadoop.fs.s3a.path.style.access','true')
           .config('spark.hadoop.fs.s3a.connection.ssl.enabled',str(ep.startswith('https:')).lower())
           .config('spark.hadoop.fs.s3a.endpoint.region',config.get('s3_region','us-east-1'))
           .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                   'software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider'))
        # S3 credentials must be injected into driver AND executors by the instructor.
        # Do not put keys in config files or print SparkConf wholesale.
    if os.environ.get('LAB3_PRELOADED_JARS')=='1':
        spark=b.getOrCreate()
    else:
        # hadoop-aws must match spark._jvm.org.apache.hadoop.util.VersionInfo.
        # Freeze that coordinate in the prepared image; NEVER guess in class.
        extras=config.get('extra_maven_packages',[])
        spark=configure_spark_with_delta_pip(b,extra_packages=extras).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark
