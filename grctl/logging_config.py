"""Custom logging configuration for Ground Control."""

import logging
import sys


class CustomFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # Format: LEVEL TIME METHOD:LINE_NUM MESSAGE
        timestamp = self.formatTime(record, "%H:%M:%S")

        level = record.levelname

        # Get class/function context
        context = f"{record.module}"
        if record.funcName != "<module>":
            context = f"{context}.{record.funcName}:{record.lineno}"

        # Format the message
        msg = f"{level} {timestamp} [{context}] {record.getMessage()}"

        # Add exception info if present
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)

        return msg


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger with custom formatter."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = CustomFormatter()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the custom formatter already configured."""
    logger = logging.getLogger(name)

    # Set logger level to DEBUG to allow all messages through
    # Actual filtering happens at handler level
    logger.setLevel(logging.DEBUG)

    return logger
