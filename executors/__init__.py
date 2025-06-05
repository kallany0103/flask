# tasks. __init__.py
import os
from dotenv import load_dotenv
from config import create_app
from .python import execute as run_script
from .bash import execute as bash_script
from .stored_procedure import execute as execute_procedure
from .stored_function import execute as execute_function
from .http import execute as http_request
from .extensions import db

# load_dotenv()
# Define the path where the .env file is stored
ENV_PATH = "/d01/def/app/server/.server_env"

# Load the .env file (check if it exists before loading it)
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    print(f"Error: The .env file was not found at {ENV_PATH}")

secret_key = os.getenv('JWT_SECRET_ACCESS_TOKEN')
database_url = os.getenv("DATABASE_URL")
print(f"database_url: {database_url}")

flask_app = create_app()
flask_app.config['SECRET_KEY'] = secret_key
flask_app.config["SQLALCHEMY_DATABASE_URI"] = database_url
db.init_app(flask_app)
celery_app = flask_app.extensions["celery"]
