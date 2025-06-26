"""With this class we can write to the standard output in zipped files.
The output is done with the command logger.info("") or logger.error("")"""
from pyspark.sql import SparkSession


class Logger:
    def __init__(self, spark: SparkSession, name: str = __name__, level: str = 'INFO'):
        """
        Initializes the Logger object.

        :param spark: Spark session object.
        :param name: Name of the logger.
        :param level: Logging level ('INFO', 'ERROR', 'WARNING', 'DEBUG').
        """
        self.printing = self.is_jupyter_notebook()
        self.name = name
        self.spark = spark

        self.log4 = self.spark._jvm.org.apache.log4j
        self.log_manager = self.log4.LogManager
        self.logger = self.log_manager.getLogger(name)

        self.set_log_level(level.upper())

    def set_log_level(self, level: str) -> None:
        """
        Sets the log level using Log4j.

        :param level: Logging level ('INFO', 'ERROR', 'WARNING', 'DEBUG').
        """
        level = level.upper()
        if level == 'INFO':
            self.logger.setLevel(self.log4.Level.INFO)
        elif level == 'ERROR':
            self.logger.setLevel(self.log4.Level.ERROR)
        elif level == 'WARNING':
            self.logger.setLevel(self.log4.Level.WARN)
        else:
            self.logger.setLevel(self.log4.Level.DEBUG)

    def print_message(self, text: str, level: str) -> None:
        """
        Logs a message or prints it if in a Jupyter notebook.

        :param text: The message to log or print.
        :param level: The logging level ('INFO', 'ERROR', 'WARNING').
        """
        level = level.upper()

        if self.printing:
            print(f"{self.name} - {level} - {text}")
        else:
            self.set_log_level(level)

            border = "________________________________________"
            message = str(text)

            if level == 'INFO':
                self.logger.info(border)
                self.logger.info(message)
                self.logger.info(border)
            elif level == 'ERROR':
                self.logger.error(border)
                self.logger.error(message)
                self.logger.error(border)
            elif level == 'WARNING':
                self.logger.warn(border)
                self.logger.warn(message)
                self.logger.warn(border)

    def info(self, text: str) -> None:
        """
        Logs an info-level message.

        :param text: Message to log.
        """
        self.print_message(text, 'INFO')

    def error(self, text: str) -> None:
        """
        Logs an error-level message.

        :param text: Message to log.
        """
        self.print_message(text, 'ERROR')

    def warning(self, text: str) -> None:
        """
        Logs a warning-level message.

        :param text: Message to log.
        """
        self.print_message(text, 'WARNING')

    @staticmethod
    def get_logger_level_object(level: str) -> str:
        """
        Converts level string to uppercase Log4j-compatible level.

        :param level: Logging level as string.
        :return: Uppercase log level string.
        """
        return level.upper()

    @staticmethod
    def is_jupyter_notebook() -> bool:
        """
        Detects if code is running inside a Jupyter notebook.

        :return: True if running in notebook, False otherwise.
        """
        try:
            from IPython import get_ipython
            cfg = get_ipython().config
            return cfg.get('IPKernelApp') is not None
        except (NameError, ImportError, KeyError, AttributeError):
            return False
