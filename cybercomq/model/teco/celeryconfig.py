BROKER_HOST = "fire.rccc.ou.edu"
BROKER_PORT = 5672
BROKER_USER = "jduckles"
BROKER_PASSWORD = "cybercommons"
BROKER_VHOST = "cybercom_test"

CELERY_RESULT_BACKEND = "mongodb"
CELERY_MONGODB_BACKEND_SETTINGS = {
    "host": "fire.rccc.ou.edu",
    "database": "cybercom_queue",
    "taskmeta_collection": "cybercom_queue_meta"
}

CELERY_IMPORTS = ("tasks","teco",)
