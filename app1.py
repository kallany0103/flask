from flask import Flask, request, jsonify, make_response
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
import os

# Define the path where the .env file is stored
#ENV_PATH = "/d01/def/app/server/.server_env"

# Load the .env file (check if it exists before loading it)
#if os.path.exists(ENV_PATH):
#    load_dotenv(ENV_PATH)


#else:
#    print(f"Error: The .env file was not found at {ENV_PATH}")

load_dotenv()
# Retrieve the database URL from environment variables
database_url_01 = os.getenv("DATABASE_URL")

# Check if the database URL is properly loaded
if not database_url_01:
    print("Error: The 'database_url' environment variable is not set.")
else:
    print(f"Database URL: {database_url_01}")

# Initialize Flask app
app = Flask(__name__)

# Configure SQLAlchemy with the database URL
app.config['SQLALCHEMY_DATABASE_URI'] = database_url_01
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Optional: Disable tracking modifications to save memory

# Initialize SQLAlchemy object
db = SQLAlchemy(app)

# Test route
@app.route('/')
def hello_world():
    return 'Hello, World!'

if __name__ == '__main__':
    app.run(debug=True)
    
