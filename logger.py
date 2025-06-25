"""With this class we can write to the standard output in zipped files.
The output is done with the command logger.info("") or logger.error("")"""
from pyspark.sql import SparkSession


class Logger:
    def __init__(self, spark: SparkSession, name: str = __name__, level: str = 'INFO'):
        self.printing = self.is_jupyter_notebook()
        self.name = name
        self.spark = spark

        self.log4 = self.spark._jvm.org.apache.log4j

        level = self.get_logger_level_object(level)
        self.log_manager = self.log4.LogManager
        self.logger = self.log_manager.getLogger(name)
        self.set_log_level(level)

    def set_log_level(self, level):
        """Sets the log level using Log4j."""
        if level == 'INFO':
            self.logger.setLevel(self.spark._jvm.org.apache.log4j.Level.INFO)
        elif level == 'ERROR':
            self.logger.setLevel(self.spark._jvm.org.apache.log4j.Level.ERROR)
        elif level == 'WARNING':
            self.logger.setLevel(self.spark._jvm.org.apache.log4j.Level.WARN)
        else:
            self.logger.setLevel(self.spark._jvm.org.apache.log4j.Level.DEBUG)

    def print_message(self, text: str, level: str):
        """Logs message, or prints it if in a Jupyter notebook."""
        if self.printing:
            print(f"{self.name} - {level} - {text}")
        else:
            self.set_log_level(level)
            if level == 'INFO':
                self.logger.info("________________________________________")
                self.logger.info(str(text))
                self.logger.info("________________________________________")
            elif level == 'ERROR':
                self.logger.error("________________________________________")
                self.logger.error(str(text))
                self.logger.error("________________________________________")
            elif level == 'WARNING':
                self.logger.warn("________________________________________")
                self.logger.warn(str(text))
                self.logger.warn("________________________________________")

    def info(self, text):
        """Logs an info message, or prints it if in a Jupyter notebook."""
        self.print_message(text, 'INFO')

    def error(self, text):
        """Logs an error message, or prints it if in a Jupyter notebook."""
        self.print_message(text, 'ERROR')

    def warning(self, text):
        """Logs a warning message, or prints it if in a Jupyter notebook."""
        self.print_message(text, 'WARNING')

    @staticmethod
    def get_logger_level_object(level):
        """Converts the level name to a logging level object (e.g., 'INFO' to logging.INFO)."""
        return level.upper()

    @staticmethod
    def is_jupyter_notebook():
        """Checks if the code is running inside a Jupyter notebook."""
        try:
            from IPython import get_ipython
            cfg = get_ipython().config
            return cfg['IPKernelApp'] is not None
        except (NameError, ImportError, KeyError, AttributeError):
            return False
