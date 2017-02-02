import logging


def always_log_info(msg):
    """Method to ALWAYS log something as info, regardless of the global log level

        This is required because boto3 will log a lot at INFO on its own

    Args:
        msg(str): The message to log

    Returns:
        None
    """
    logger = logging.getLogger('ingest-client')
    current_level = logger.getEffectiveLevel()
    logger.setLevel(logging.INFO)
    logger.info(msg)
    logger.setLevel(current_level)
