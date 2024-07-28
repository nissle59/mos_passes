import json
import logging
import logging.config
import sys

log_config = json.load(open('logging.json', 'r'))
log_config['handlers']['http']['host'] = 'localhost:50000'
logging.config.dictConfig(log_config)
LOGGER = logging.getLogger(__name__)

AUTH_IMAGE = 'mos-auth-local'

class MQ:
    # host = 'localhost'
    # host = 'rmq.db.services.local'
    host = 'localhost'
    user = 'rmuser'
    password = 'rmpassword'
    port = 5672
    apiport = 15672
    vhost = 'egts'
    queue = 'urgent_q'


DSN = 'postgresql://postgres:psqlpass@localhost/vindcgibdd'
#DSN = 'postgresql://postgres:psqlpass@10.8.0.5/vindcgibdd'

