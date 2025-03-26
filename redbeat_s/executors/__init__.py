# tasks. __init__.py

from config import create_app 
from .stored_procedure import execute
from .python import execute as run_script
from.stored_procedure import execute as execute_procedure
from.stored_function import execute as execute_function
from .extensions import db
from .bash import execute as run_bash_script
# flask_app = create_app()  
# celery_app = flask_app.extensions["celery"] 
# #celery =celery_app

flask_app = create_app() 
flask_app.config["SQLALCHEMY_DATABASE_URI"] = "postgresql://postgres:postgres@172.16.13.232:5434/celery_explore?sslmode=require"
db.init_app(flask_app)
celery_app = flask_app.extensions["celery"] 


