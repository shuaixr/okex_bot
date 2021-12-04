import logging
import telebot


class TelegramHandler(logging.Handler):
    def __init__(self, token: str, id: str, level: logging._Level = ...) -> None:
        self.bot = telebot.TeleBot(token=token)
        self.id = id
        super().__init__(level=level)

    def emit(self, record: logging.LogRecord) -> None:
        self.bot.send_message(self.id, self.format(record=record))


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


def set_telegram_log(token: str, id: str):
    telelog = TelegramHandler(token, id)
    telelog.setFormatter(formatter)
    rootLogger.addHandler(telelog)
