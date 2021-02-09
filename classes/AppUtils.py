from requests import get
from requests.exceptions import RequestException
from contextlib import closing
import logging
import sys
from datetime import *

logger = logging.getLogger(__name__)


def configure_log(main_name):
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.addHandler(
        logging.FileHandler("logs/{0}_{1}.log".format(str(datetime.now().strftime("%Y%m%d_%H%M%S")), main_name)))
    logger.setLevel(logging.DEBUG)


def simple_get(url):
    try:
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        logger.error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None


def is_good_response(resp):
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200
            and content_type is not None
            and content_type.find('html') > -1)
