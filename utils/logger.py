class SparkLogger(object):

    def __init__(self, spark, log_level):
        self.log4j_logger = spark._jvm.org.apache.log4j
        self.conf = spark.sparkContext.getConf()
        spark.sparkContext.setLogLevel(log_level)

    def get_logger(self, logger_name=None):
        """
        Get log4j logger for logging on spark
        :param logger_name:
        :return:
        """
        if not logger_name:
            app_name = self.conf.get('spark.app.name')
            app_id = self.conf.get('spark.app.id')
            logger_name = f'[{app_name}][{app_id}]'

        return self.log4j_logger.LogManager.getLogger(logger_name)
