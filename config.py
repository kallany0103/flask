#config.py
from celery import Celery, Task  # Import Celery and Task classes for asynchronous task management
from flask import Flask          # Import Flask to create and manage a web application
import redis                     # Import Redis library for broker communication
import psycopg2                  # Import psycopg2 to connect with PostgreSQL database
import os                        # Import os for environment variable handling
from dotenv import load_dotenv   # Import load_dotenv to load environment variables from a .env file
import ssl


# Load environment variables from the .env file
# load_dotenv()  # This loads the variables defined in the .env file into the environment


# Define the path where the .env file is stored
ENV_PATH = "/d01/def/app/server/.server_env"

# Load the .env file (check if it exists before loading it)
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    print(f"Error: The .env file was not found at {ENV_PATH}")


# Fetch Redis and database URLs from environment variables
redis_url = os.environ.get("MESSAGE_BROKER")         # Redis URL for Celery's message broker
database_url = os.environ.get("DATABASE_URL")   # PostgreSQL URL for Celery's result backend


# Function to initialize and configure Celery with Flask
def celery_init_app(app: Flask) -> Celery:
    # Define a custom Celery Task class that runs tasks in Flask's application context
    class FlaskTask(Task):
        def __call__(self, *args: object, **kwargs: object) -> object:
            # Use Flask's app context to ensure proper access to app resources
            with app.app_context():
                return self.run(*args, **kwargs)  # Call the task's run method with arguments

    # Create a Celery instance, associating it with the Flask app name and custom task class
    celery_app = Celery(app.name, task_cls=FlaskTask)
    
    # Configure Celery using the Flask app's configuration
    celery_app.config_from_object(app.config["CELERY"])

    # Manually apply SSL config if needed
    if "broker_use_ssl" in app.config["CELERY"]:
        celery_app.conf.broker_use_ssl = app.config["CELERY"]["broker_use_ssl"]

    
    # Set the created Celery app as the default instance
    celery_app.set_default()
    
    # Store the Celery instance in Flask's extensions for easy access in the app
    app.extensions["celery"] = celery_app
    
    # Return the configured Celery instance
    return celery_app


# Function to create and configure a Flask app
def create_app() -> Flask:
    # Create a Flask application instance
    app = Flask(__name__)
    
    # Configure the app with Celery settings using a dictionary
    # app.config.from_mapping(
    #     CELERY=dict(
    #         broker_url=redis_url,                      # Redis as the message broker
    #         result_backend="db+"+database_url,              # PostgreSQL as the result backend
    #         #result_backend=database_url,              # PostgreSQL as the result backend
    #         beat_scheduler='redbeat.RedBeatScheduler',# RedBeat scheduler for periodic tasks
    #         redbeat_redis_url=redis_url,              # Redis URL for RedBeat configuration
    #         timezone='UTC',                           # Use UTC timezone for tasks
    #         enable_utc=True                          # Enable UTC mode
    #     ),
    # )
    app.config.from_mapping(
        CELERY=dict(
            broker_url=redis_url,                      # Redis as the message broker
            result_backend="db+"+database_url,              # PostgreSQL as the result backend
            #result_backend=database_url,              # PostgreSQL as the result backend
            beat_scheduler='redbeat.RedBeatScheduler',# RedBeat scheduler for periodic tasks
            redbeat_redis_url=redis_url,              # Redis URL for RedBeat configuration
            redbeat_lock_timeout=300,
            # broker_use_ssl = {
            #     'ssl_cert_reqs': ssl.CERT_NONE  # or ssl.CERT_REQUIRED if you have proper certs
            # },
            timezone='UTC',                           # Use UTC timezone for tasks
            enable_utc=True                          # Enable UTC mode
        ),
    )
    # Load additional configuration from environment variables with a prefix
    app.config.from_prefixed_env()
    
    # Initialize Celery with the Flask app
    celery_init_app(app)
    
    # Return the fully configured Flask app
    return app
