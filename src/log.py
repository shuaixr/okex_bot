import logging
import telebot


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


class TelegramHandler(logging.Handler):
    def __init__(self, token: str, id: str, level=logging.NOTSET) -> None:
        self.bot = telebot.TeleBot(token=token)
        self.id = id
        super(TelegramHandler, self).__init__(level=level)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.bot.send_message(self.id, self.format(record=record))
        except Exception as e:
            print(str(e))


def set_telegram_log(token: str, id: str):
    telelog = TelegramHandler(token, id)
    telelog.setFormatter(formatter)
    rootLogger.addHandler(telelog)
