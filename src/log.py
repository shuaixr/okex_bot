import logging

"""
class TelegramHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        return super().emit(record)
"""
rootLogger = logging.getLogger()

formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
fileHandler = logging.FileHandler("logger.log")
fileHandler.setFormatter(formatter)
rootLogger.addHandler(fileHandler)

streamHandler = logging.StreamHandler()
streamHandler.setFormatter(formatter)
rootLogger.addHandler(streamHandler)

logger = logging.getLogger("okex_bot")
logger.setLevel(level=logging.DEBUG)
