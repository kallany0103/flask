#app.py
import os 
import json
import uuid
import requests
import traceback
import logging
from itertools import count
from functools import wraps
from flask_cors import CORS 
from dotenv import load_dotenv            # To load environment variables from a .env file
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine, text, desc, cast, TIMESTAMP
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, make_response       # Flask utilities for handling requests and responses
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from werkzeug.security import generate_password_hash, check_password_hash
from executors import flask_app # Import Flask app and tasks
from executors.extensions import db
from celery import current_app as celery  # Access the current Celery app
from executors.models import DefAsyncTask, DefAsyncTaskParam, DefAsyncTaskSchedule, DefAsyncTaskRequest, DefAsyncTaskSchedulesV, DefAsyncExecutionMethods, DefAsyncTaskScheduleNew, DefTenant, DefUser, DefPerson, DefUserCredential, DefAccessProfile, DefUsersView, Message, DefTenantEnterpriseSetup
from redbeat_s.red_functions import create_redbeat_schedule, update_redbeat_schedule, delete_schedule_from_redis
from ad_hoc.ad_hoc_functions import execute_ad_hoc_task, execute_ad_hoc_task_v1
from celery.schedules import crontab
from celery.result import AsyncResult      # For checking the status of tasks
from redbeat import RedBeatSchedulerEntry


jwt = JWTManager(flask_app)
CORS(flask_app)
# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def generate_user_id():
    try:
        # Query the max user_id from the arc_users table
        max_user_id = db.session.query(db.func.max(DefUser.user_id)).scalar()
        if max_user_id is not None:
            # If max_user_id is not None, set the start value for the counter
            user_id_counter = count(start=max_user_id + 1)
        else:
            # If max_user_id is None, set a default start value
            user_id_counter = count(start=int(datetime.timestamp(datetime.utcnow())))
        
        # Generate a unique user_id using the counter
        return next(user_id_counter)
    except Exception as e:
        print(f"Error generating user ID: {e}")
        return None



# def generate_tenant_id():
#     try:
#         # Query the max tenant_id from the arc_tenants table
#         max_tenant_id = db.session.query(db.func.max(DefTenantEnterpriseSetup.tenant_id)).scalar()
#         if max_tenant_id is not None:
#             # If max_tenant_id is not None, set the start value for the counter
#             tenant_id_counter = count(start=max_tenant_id + 1)
#         else:
#             # If max_tenant_id is None, set a default start value based on the current timestamp
#             tenant_id_counter = count(start=int(datetime.timestamp(datetime.utcnow())))

#         # Generate a unique tenant_id using the counter
#         return next(tenant_id_counter)
#     except Exception as e:
#         # Handle specific exceptions as needed
#         print(f"Error generating tenant ID: {e}")
#         return None



# def generate_tenant_enterprise_id():
#     try:
#         # Query the max tenant_id from the arc_tenants table
#         max_tenant_id = db.session.query(db.func.max(DefTenantEnterpriseSetup.tenant_id)).scalar()
#         if max_tenant_id is not None:
#             # If max_tenant_id is not None, set the start value for the counter
#             tenant_id_counter = count(start=max_tenant_id + 1)
#         else:
#             # If max_tenant_id is None, set a default start value based on the current timestamp
#             tenant_id_counter = count(start=int(datetime.timestamp(datetime.utcnow())))

#         # Generate a unique tenant_id using the counter
#         return next(tenant_id_counter)
#     except Exception as e:
#         # Handle specific exceptions as needed
#         print(f"Error generating tenant ID: {e}")
#         return None

def current_timestamp():
    return datetime.now().strftime('%d-%m-%Y %H:%M:%S')



@flask_app.route('/messages/<string:id>', methods=['GET'])
def get_reply_message(id):
    try:
        # Query the database for messages with the given parentid
        messages = Message.query.filter_by(parentid=id).order_by(desc(Message.date)).all()
        
        # Check if any messages were found
        if messages:
            return make_response(jsonify([msg.json() for msg in messages]), 200)
        else:
            return jsonify({'message': 'MessageID not found.'}), 404
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

@flask_app.route('/messages', methods=['GET'])
def get_messages():
    try:
        # Query the database for messages with the given parentid
        messages = Message.query.all()
        
        # Check if any messages were found
        if messages:
            return make_response(jsonify([msg.json() for msg in messages]), 200)
        else:
            return jsonify({'message': 'MessageID not found.'}), 404
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    
@flask_app.route('/messages', methods=['POST'])
def create_message():
    try:
        data = request.get_json()
        id            = data['id']
        sender        = data['sender']
        recivers      = data['recivers']
        subject       = data['subject']
        body          = data['body']
        date          = data['date'],
        status        = data['status']
        parentid      = data['parentid'],
        involvedusers = data['involvedusers']
        readers       = data['readers']
        
        receiver_json = json.dumps(recivers)
        involvedusers = json.dumps(involvedusers)
        
        new_message = Message(
            id            = id,
            sender        = sender,
            recivers      = recivers,
            subject       = subject,
            body          = body,
            date          = date,
            status        = status,
            parentid      = parentid,
            involvedusers = involvedusers,
            readers       = readers
        )   
        
        db.session.add(new_message)
        db.session.commit()
        
        return make_response(jsonify({"Message": "Message sent Successfully"}, 201))    
    except Exception as e:
        return make_response(jsonify({"Message": f"Error: {str(e)}"}), 500)
    
    
@flask_app.route('/messages/<string:id>', methods=['PUT'])
def update_messages(id):
    try:
        message = Message.query.filter_by(id=id).first()
        if message:
            data = request.get_json()
            message.subject  = data['subject']
            message.body = data['body']
            db.session.commit()
            return make_response(jsonify({"message": "Message updated successfully"}), 200)
        return make_response(jsonify({"message": "Message not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error updating Message", "error": str(e)}), 500)
    

@flask_app.route('/messages/<string:id>', methods=['DELETE'])
def delete_message(id):
    try:
        message = Message.query.filter_by(id=id).first()
        if message:
            db.session.delete(message)
            db.session.commit()
            return make_response(jsonify({"message": "Message deleted successfully"}), 200)
        return make_response(jsonify({"message": "Message not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error deleting message", "error": str(e)}), 500)




# Create enterprise setup
@flask_app.route('/create_enterprise/<int:tenant_id>', methods=['POST'])
def create_enterprise(tenant_id):
    try:
        data = request.get_json()
        tenant_id       = tenant_id
        enterprise_name = data['enterprise_name']
        enterprise_type = data['enterprise_type']

        new_enterprise = DefTenantEnterpriseSetup(
            tenant_id=tenant_id,
            enterprise_name=enterprise_name,
            enterprise_type=enterprise_type
        )

        db.session.add(new_enterprise)
        db.session.commit()
        return make_response(jsonify({"message": "Enterprise setup created successfully"}), 201)

    except IntegrityError:
        return make_response(jsonify({"message": "Error creating enterprise setup", "error": "Setup already exists"}), 409)
    except Exception as e:
        return make_response(jsonify({"message": "Error creating enterprise setup", "error": str(e)}), 500)


# Get all enterprise setups
@flask_app.route('/get_enterprises', methods=['GET'])
def get_enterprises():
    try:
        setups = DefTenantEnterpriseSetup.query.all()
        return make_response(jsonify([setup.json() for setup in setups]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving enterprise setups", "error": str(e)}), 500)


# Get one enterprise setup by tenant_id
@flask_app.route('/get_enterprise/<int:tenant_id>', methods=['GET'])
def get_enterprise(tenant_id):
    try:
        setup = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()
        if setup:
            return make_response(jsonify(setup.json()), 200)
        return make_response(jsonify({"message": "Enterprise setup not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving enterprise setup", "error": str(e)}), 500)


# Update enterprise setup
@flask_app.route('/update_enterprise/<int:tenant_id>', methods=['PUT'])
def update_enterprise(tenant_id):
    try:
        setup = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()
        if setup:
            data = request.get_json()
            setup.enterprise_name = data.get('enterprise_name', setup.enterprise_name)
            setup.enterprise_type = data.get('enterprise_type', setup.enterprise_type)
            db.session.commit()
            return make_response(jsonify({"message": "Enterprise setup updated successfully"}), 200)
        return make_response(jsonify({"message": "Enterprise setup not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error updating enterprise setup", "error": str(e)}), 500)


# Delete enterprise setup
@flask_app.route('/delete_enterprise/<int:tenant_id>', methods=['DELETE'])
def delete_enterprise(tenant_id):
    try:
        setup = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()
        if setup:
            db.session.delete(setup)
            db.session.commit()
            return make_response(jsonify({"message": "Enterprise setup deleted successfully"}), 200)
        return make_response(jsonify({"message": "Enterprise setup not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error deleting enterprise setup", "error": str(e)}), 500)

 

# Create a tenant
@flask_app.route('/tenants', methods=['POST'])
def create_tenant():
    try:
       data = request.get_json()
    #    tenant_id   = generate_tenant_id()  # Call the function to get the result
       tenant_name = data['tenant_name']
       new_tenant  = DefTenant(tenant_name = tenant_name)
       db.session.add(new_tenant)
       db.session.commit()
       return make_response(jsonify({"message": "Tenant created successfully"}), 201)
   
    except IntegrityError as e:
        return make_response(jsonify({"message": "Error creating Tenant", "error": "Tenant already exists"}), 409)
    except Exception as e:
        return make_response(jsonify({"message": "Error creating Tenant", "error": str(e)}), 500)

        

# Get all tenants
@flask_app.route('/tenants', methods=['GET'])
def get_tenants():
    try:
        tenants = DefTenant.query.all()
        return make_response(jsonify([tenant.json() for tenant in tenants]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Tenants", "error": str(e)}), 500)


@flask_app.route('/tenants/<int:tenant_id>', methods=['GET'])
def get_tenant(tenant_id):
    try:
        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if tenant:
            return make_response(jsonify(tenant.json()),201)
        else:
            return make_response(jsonify({"message": "Tenant not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving tenant", "error": str(e)}), 500)


# Update a tenant
@flask_app.route('/tenants/<int:tenant_id>', methods=['PUT'])
def update_tenant(tenant_id):
    try:
        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if tenant:
            data = request.get_json()
            tenant.tenant_name  = data['tenant_name']
            db.session.commit()
            return make_response(jsonify({"message": "Tenant updated successfully"}), 200)
        return make_response(jsonify({"message": "Tenant not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error updating Tenant", "error": str(e)}), 500)


# Delete a tenant
@flask_app.route('/tenants/<int:tenant_id>', methods=['DELETE'])
def delete_tenant(tenant_id):
    try:
        user = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return make_response(jsonify({"message": "Tenant deleted successfully"}), 200)
        return make_response(jsonify({"message": "Tenant not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error deleting tenant", "error": str(e)}), 500)



@flask_app.route('/defusers', methods=['POST'])
def create_def_user():
    try:
        # Parse data from the request body
        data = request.get_json()
        user_id         = generate_user_id()
        user_name       = data['user_name']
        user_type       = data['user_type']
        email_addresses = data['email_addresses']
        created_by      = data['created_by']
        created_on      = current_timestamp()
        last_updated_by = data['last_updated_by']
        last_updated_on = current_timestamp()
        tenant_id       = data['tenant_id']
        

       # Convert the list of email addresses to a JSON-formatted string
       # email_addresses_json = json.dumps(email_addresses)  # Corrected variable name

       # Create a new ArcUser object
        new_user = DefUser(
          user_id         = user_id,
          user_name       = user_name,
          user_type       = user_type,
          email_addresses = email_addresses,  # Corrected variable name
          created_by      = created_by,
          created_on      = created_on,
          last_updated_by = last_updated_by,
          last_updated_on = last_updated_on,
          tenant_id       = tenant_id
        )
        # Add the new user to the database session
        db.session.add(new_user)
        # Commit the changes to the database
        db.session.commit()

        # Return a success response
        return make_response(jsonify({"message": "Def USER created successfully!"}), 201)

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    


@flask_app.route('/defusers', methods=['GET'])
def get_users():
    try:
        users = DefUser.query.all()
        return make_response(jsonify([user.json() for user in users]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'error getting users', 'error': str(e)}), 500)
    
    
# get a user by id
@flask_app.route('/defusers/<int:user_id>', methods=['GET'])
def get_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            return make_response(jsonify({'user': user.json()}), 200)
        return make_response(jsonify({'message': 'user not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'error getting user', 'error': str(e)}), 500)
    
    
@flask_app.route('/defusers/<int:user_id>', methods=['PUT'])
def update_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            data = request.get_json()
            if 'user_name' in data:
                user.user_name = data['user_name']
            if 'email_addresses' in data:
                user.email_addresses = data['email_addresses']
            if 'last_updated_by' in data:
                user.last_updated_by = data['last_updated_by']
            user.last_updated_on = current_timestamp()
            db.session.commit()
            return make_response(jsonify({'message': 'user updated'}), 200)
        return make_response(jsonify({'message': 'user not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'error updating user', 'error': str(e)}), 500)
    
    
    
@flask_app.route('/defusers/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return make_response(jsonify({'message': 'User deleted successfully'}), 200)
        return make_response(jsonify({'message': 'user not found'}), 404)
    except:
        return make_response(jsonify({'message': 'error deleting user'}), 500)

        
    
@flask_app.route('/defpersons', methods=['POST'])
def create_arc_person():
    try:
        data = request.get_json()
        user_id     = data['user_id']
        first_name  = data['first_name']
        middle_name = data['middle_name']
        last_name   = data['last_name']
        job_title   = data['job_title']  
        
        # create arc persons object 
        person =  DefPerson(
            user_id     = user_id,
            first_name  = first_name,
            middle_name = middle_name,
            last_name   = last_name,
            job_title   = job_title
        ) 
        
        # Add arc persons data to the database session
        db.session.add(person)
        # Commit the changes to the database
        db.session.commit()
        # Return a success response
        return make_response(jsonify({"message": "Def person's data created succesfully"}), 201)
    
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    
    
@flask_app.route('/defpersons', methods=['GET'])
def get_persons():
    try:
        persons = DefPerson.query.all()
        return make_response(jsonify([person.json() for person in persons]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'error getting persons', 'error': str(e)}), 500)
    

@flask_app.route('/defpersons/<int:user_id>', methods=['GET'])
def get_person(user_id):
    try:
        person = DefPerson.query.filter_by(user_id=user_id).first()
        if person:
            return make_response(jsonify({'person': person.json()}), 200)
        return make_response(jsonify({'message': 'Person not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting person', 'error': str(e)}), 500) 


@flask_app.route('/defpersons/<int:user_id>', methods=['PUT'])
def update_person(user_id):
    try:
        person = DefPerson.query.filter_by(user_id=user_id).first()
        if person:
            data = request.get_json()
            # Update only the fields provided in the JSON data
            if 'first_name' in data:
                person.first_name = data['first_name']
            if 'middle_name' in data:
                person.middle_name = data['middle_name']
            if 'last_name' in data:
                person.last_name = data['last_name']
            if 'job_title' in data:
                person.job_title = data['job_title']
            db.session.commit()
            return make_response(jsonify({'message': 'person updated'}), 200)
        return make_response(jsonify({'message': 'person not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'error updating person', 'error': str(e)}), 500)
    
    
@flask_app.route('/defpersons/<int:user_id>', methods=['DELETE'])
def delete_person(user_id):
    try:
        person = DefPerson.query.filter_by(user_id=user_id).first()
        if person:
            db.session.delete(person)
            db.session.commit()
            return make_response(jsonify({'message': 'Person deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Person not found'}), 404)
    except:
        return make_response(jsonify({'message': 'Error deleting user'}), 500)


    
@flask_app.route('/def_user_credentials', methods=['POST'])
def create_user_credential():
    try:
        # Parse data from the request body
        data = request.get_json()
        user_id  = data['user_id']
        password = data['password']

        # Create a new DefUserCredentials object
        credential = DefUserCredential(
            user_id  = user_id,
            password = password
        )

        # Add the new credentials to the database session
        db.session.add(credential)
        # Commit the changes to the database
        db.session.commit()

        # Return a success response
        return make_response(jsonify({"message": "User credentials created successfully!"}), 201)

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    


    
    
@flask_app.route('/reset_user_password', methods=['PUT'])
#@jwt_required()
def reset_user_password():
    #current_user_id = get_jwt_identity()
    data = request.get_json()
    current_user_id = data['user_id']
    old_password = data['old_password']
    new_password = data['new_password']
    
    user = DefUserCredential.query.get(current_user_id)
    if not user:
        return jsonify({'message': 'User not found'}), 404
    
    if not check_password_hash(user.password, old_password):
        return jsonify({'message': 'Invalid old password'}), 401
    
    hashed_new_password = generate_password_hash(new_password, method='pbkdf2:sha256', salt_length=16)
    user.password = hashed_new_password
    
    db.session.commit()
    
    return jsonify({'message': 'Password reset successful'}), 200


@flask_app.route('/def_user_credentials/<int:user_id>', methods=['DELETE'])
def delete_user_credentials(user_id):
    try:
        credential = DefUserCredential.query.filter_by(user_id=user_id).first()
        if credential:
            db.session.delete(credential)
            db.session.commit()
            return make_response(jsonify({'message': 'User credentials deleted successfully'}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except:
        return make_response(jsonify({'message': 'Error deleting user credentials'}), 500)
  
    
@flask_app.route('/users', methods=['POST'])
#@jwt_required()
#@role_required('/users', 'POST')
def create_user():
    try:
        data = request.get_json()
        user_id         = generate_user_id()
        user_name       = data['user_name']
        user_type       = data['user_type']
        email_addresses = data['email_addresses']
        first_name      = data['first_name']
        middle_name     = data['middle_name']
        last_name       = data['last_name']
        job_title       = data['job_title']
        # Get user information from the JWT token
        #created_by      = get_jwt_identity()
        #last_updated_by = get_jwt_identity()
        created_by      = data['created_by']
        last_updated_by = data['last_updated_by']
        # created_on      = data['created_on']
        # last_updated_on = data['last_updated_on']
        tenant_id       = data['tenant_id']
        password        = data['password']
        # privilege_names = data.get('privilege_names', [])
        # role_names      = data.get('role_names', [])

        existing_user  = DefUser.query.filter(DefUser.user_name == user_name).first()
        # existing_user = ArcPerson.query.filter((ArcPerson.user_id == user_id) | (ArcPerson.username == username)).first()
        existing_email = DefUser.query.filter(DefUser.email_addresses == email_addresses).first()

        if existing_user:
            if existing_user.user_name == user_name:
                return make_response(jsonify({"message": "Username already exists"}), 400)
        
        if existing_email:
            return make_response(jsonify({"message": "Email address already exists"}), 409)
            
        hashed_password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)
        
        #current_timestamp = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        # Create a user record for the new user
        def_user = DefUser(
            user_id         = user_id,
            user_name       = user_name,
            user_type       = user_type,
            email_addresses = email_addresses,
            created_by      = created_by,
            created_on      = current_timestamp(),
            last_updated_by = last_updated_by,
            last_updated_on = current_timestamp(),
            tenant_id       = tenant_id
            
        )

        # Add the person record for the new user
        db.session.add(def_user)
        db.session.commit()

        # Check if user_type is "person" before creating ArcPerson record
        if user_type.lower() == "person":
            def_person_data = {
                "user_id"    : user_id,
                "first_name" : first_name,
                "middle_name": middle_name,
                "last_name"  : last_name,
                "job_title"  : job_title
            }

            def_person_response = requests.post("http://localhost:5000/defpersons", json=def_person_data)

            # Check the response from create_arc_user API
            if def_person_response.status_code != 201:
                return jsonify({"message": "Def person's record creation failed"}), 500

    
        # Create user credentials
        user_credentials_data = {
            "user_id" : user_id,
            "password": hashed_password
        }
        user_credentials_response = requests.post("http://localhost:5000/def_user_credentials", json=user_credentials_data)

        if user_credentials_response.status_code != 201:
            # Handle the case where user credentials creation failed
            return make_response(jsonify({"message": "User credentials creation failed"}), 500)


    #    # Assign privileges to the user
    #     user_privileges_data = {
    #         "user_id": user_id,
    #         "privilege_names": privilege_names
    #     }
    #     user_privileges_response = requests.post("http://localhost:5000/user_granted_privileges", json=user_privileges_data)

    #     if user_privileges_response.status_code != 201:
    #         return jsonify({'message': 'User privileges assignment failed'}), 500

    #     # Assign roles to the user
    #     user_roles_data = {
    #         "user_id": user_id,
    #         "role_names": role_names
    #     }
    #     user_roles_response = requests.post("http://localhost:5000/user_granted_roles", json=user_roles_data)

    #     if user_roles_response.status_code != 201:
    #         return jsonify({'message': 'User roles assignment failed'}), 500

    #     db.session.commit()

        return jsonify({"message": "User created successfully"}), 201
    
    except IntegrityError as e:
        return jsonify({"message": "User id already exists"}), 400  # 400 Bad Request for unique constraint violation
   
    except Exception as e:
        return jsonify({"message": str(e)}), 500
        traceback.print_exc()



@flask_app.route('/users/<int:user_id>', methods=['PUT'])
def update_specific_user(user_id):
    try:
        # Retrieve the user record from the DefUser table
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            data = request.get_json()
            
            # Update fields in DefUser table
            if 'user_name' in data:
                user.user_name = data['user_name']
            if 'user_type' in data:
                user.user_type = data['user_type']
            if 'email_addresses' in data:
                user.email_addresses = data['email_addresses']
            if 'last_updated_by' in data:
                user.last_updated_by = data['last_updated_by']
            user.last_updated_on = current_timestamp()  # Update last updated timestamp

            # Commit changes to DefUser
            db.session.commit()

            # If user_type is "person", update fields in DefPerson table
            if user.user_type.lower() == "person":
                person = DefPerson.query.filter_by(user_id=user_id).first()
                if person:
                    # Update fields in DefPerson table
                    if 'first_name' in data:
                        person.first_name = data['first_name']
                    if 'middle_name' in data:
                        person.middle_name = data['middle_name']
                    if 'last_name' in data:
                        person.last_name = data['last_name']
                    if 'job_title' in data:
                        person.job_title = data['job_title']
                    
                    # Commit changes to DefPerson
                    db.session.commit()
                    return make_response(jsonify({'message': 'User and person updated successfully'}), 200)
                return make_response(jsonify({'message': 'Person not found'}), 404)

            return make_response(jsonify({'message': 'User updated successfully'}), 200)

        return make_response(jsonify({'message': 'User not found'}), 404)

    except Exception as e:
        db.session.rollback()  # Rollback in case of an error
        return make_response(jsonify({'message': 'Error updating user', 'error': str(e)}), 500)


@flask_app.route('/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        email_or_username = data['email_or_username']
        password          = data['password']
        
        if not email_or_username or not password:
            return make_response(jsonify({"message": "Invalid request. Please provide both email/username and password."}), 400)

        # Set a default value for user_profile
        user_profile = None

        # Check if the input is an email address or a username
        if '@' in email_or_username:
        # Cast JSON column to TEXT and use LIKE
            user_profile = DefUser.query.filter(cast(DefUser.email_addresses, Text).ilike(f"%{email_or_username}%")).first()
        else:
            user_profile = DefUser.query.filter_by(user_name = email_or_username).first()
        if user_profile and user_profile.user_id:
            user_credentials = DefUserCredential.query.filter_by(user_id = user_profile.user_id).first()

            if user_credentials and check_password_hash(user_credentials.password, password):
                access_token = create_access_token(identity = user_profile.user_id)
                return make_response(jsonify({"access_token": access_token}), 200)
            else:
                return make_response(jsonify({"message": "Invalid email/username or password"}), 401)
        else:
            return make_response(jsonify({"message": "User not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": str(e)}), 500)
        
    
@flask_app.route('/users', methods=['GET'])
def defusers():
    try:
        defusers = DefUsersView.query.all()
        return make_response(jsonify([defuser.json() for defuser in defusers]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting users', 'error': str(e)}), 500)
    
    
@flask_app.route('/users/<int:user_id>', methods=['GET'])
def get_specific_user(user_id):
    try:
        user = DefUsersView.query.filter_by(user_id=user_id).first()
        if user:
            return make_response(jsonify({'user': user.json()}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting User', 'error': str(e)}), 500)  
    
    
@flask_app.route('/users/<int:user_id>', methods=['DELETE'])
def delete_specific_user(user_id):
    try:
        # Find the user record in the DefUser table
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            # Delete the DefPerson record if it exists and user_type is "person"
            if user.user_type.lower() == "person":
                person = DefPerson.query.filter_by(user_id=user_id).first()
                if person:
                    db.session.delete(person)

            # Delete the DefUserCredential record if it exists
            user_credential = DefUserCredential.query.filter_by(user_id=user_id).first()
            if user_credential:
                db.session.delete(user_credential)

            # Delete the DefUser record
            db.session.delete(user)
            db.session.commit()

            return make_response(jsonify({'message': 'User and related records deleted successfully'}), 200)

        return make_response(jsonify({'message': 'User not found'}), 404)
    
    except Exception as e:
        db.session.rollback()  # Rollback in case of any error
        return make_response(jsonify({'message': 'Error deleting user', 'error': str(e)}), 500)
  

@flask_app.route('/access_profiles/<int:user_id>', methods=['POST'])
def create_access_profiles(user_id):
    try:
        profile_type = request.json.get('profile_type')  # Fixed incorrect key
        profile_id = request.json.get('profile_id')
        primary_yn = request.json.get('primary_yn', 'N')  # Default to 'N' if not provided

        if not profile_type or not profile_id:
            return make_response(jsonify({"message": "Missing required fields"}), 400)

        new_profile = DefAccessProfile(
            user_id=user_id,
            profile_type=profile_type,
            profile_id=profile_id,
            primary_yn=primary_yn
        )

        db.session.add(new_profile)  # Fixed: Corrected session operation
        db.session.commit()
        return make_response(jsonify({"message": "Access profiles created successfully"}), 201)

    except IntegrityError as e:
        db.session.rollback()  
        print("IntegrityError:", str(e))  
        return make_response(jsonify({"message": "Error creating Access Profiles", "error": str(e)}), 409)

    except Exception as e:
        db.session.rollback()  
        print("General Exception:", str(e))  
        return make_response(jsonify({"message": "Error creating Access Profiles", "error": str(e)}), 500)


# Get all access profiles
@flask_app.route('/access_profiles', methods=['GET'])
def get_users_access_profiles():
    try:
        profiles = DefAccessProfile.query.all()
        return make_response(jsonify([profile.json() for profile in profiles]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Access Profiles", "error": str(e)}), 500)


@flask_app.route('/access_profiles/<int:user_id>', methods=['GET'])
def get_user_access_profiles_(user_id):
    try:
        profiles = DefAccessProfile.query.filter_by(user_id=user_id).all()
        
        if profiles:
            return make_response(jsonify([profile.json() for profile in profiles]), 200)
        else:
            return make_response(jsonify({"message": "Access Profiles not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving Access Profiles", "error": str(e)}), 500)


@flask_app.route('/access_profiles/<int:user_id>/<int:serial_number>', methods=['PUT'])
def update_access_profile(user_id, serial_number):
    try:
        # Retrieve the existing access profile
        profile = DefAccessProfile.query.filter_by(user_id=user_id, serial_number=serial_number).first()
        if not profile:
            return make_response(jsonify({"message": "Access Profile not found"}), 404)

        data = request.get_json()

        # Update fields in DefAccessProfile table
        if 'profile_type' in data:
            profile.profile_type = data['profile_type']
        if 'profile_id' in data:
            profile.profile_id = data['profile_id']
        if 'primary_yn' in data:
            profile.primary_yn = data['primary_yn']

        # Commit changes to DefAccessProfile
        db.session.commit()

        return make_response(jsonify({"message": "Access Profile updated successfully"}), 200)

    except Exception as e:
        db.session.rollback()  # Rollback on error
        return make_response(jsonify({"message": "Error updating Access Profile", "error": str(e)}), 500)


# Delete an access profile
@flask_app.route('/access_profiles/<int:user_id>/<int:serial_number>', methods=['DELETE'])
def delete_access_profile(user_id, serial_number):
    try:
        profile = DefAccessProfile.query.filter_by(user_id=user_id, serial_number=serial_number).first()
        if profile:
            db.session.delete(profile)
            db.session.commit()
            return make_response(jsonify({"message": "Access Profile deleted successfully"}), 200)
        return make_response(jsonify({"message": "Access Profile not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error deleting Access Profile", "error": str(e)}), 500)




@flask_app.route('/Create_ExecutionMethod', methods=['POST'])
def Create_ExecutionMethod():
    try:
        execution_method = request.json.get('execution_method')
        internal_execution_method = request.json.get('internal_execution_method')
        executor = request.json.get('executor')
        description = request.json.get('description')

        # Validate required fields
        if not execution_method or not internal_execution_method:
            return jsonify({"error": "Missing required fields: execution_method or internal_execution_method"}), 400

        # Check if the execution method already exists
        existing_method = DefAsyncExecutionMethods.query.filter_by(internal_execution_method=internal_execution_method).first()
        if existing_method:
            return jsonify({"error": f"Execution method '{internal_execution_method}' already exists"}), 409

        # Create a new execution method object
        new_method = DefAsyncExecutionMethods(
            execution_method=execution_method,
            internal_execution_method=internal_execution_method,
            executor=executor,
            description=description
        )

        # Add to session and commit
        db.session.add(new_method)
        db.session.commit()

        return jsonify({"message": "Execution method created successfully", "data": new_method.json()}), 201

    except Exception as e:
        return jsonify({"message": "Error creating execution method", "error": str(e)}), 500



@flask_app.route('/Show_ExecutionMethods', methods=['GET'])
def Show_ExecutionMethods():
    try:
        methods = DefAsyncExecutionMethods.query.all()
        if not methods:
            return jsonify({"message": "No execution methods found"}), 404
        return jsonify([method.json() for method in methods]), 200
    except Exception as e:
        return jsonify({"message": "Error retrieving execution methods", "error": str(e)}), 500


@flask_app.route('/Show_ExecutionMethod/<string:internal_execution_method>', methods=['GET'])
def Show_ExecutionMethod(internal_execution_method):
    try:
        method = DefAsyncExecutionMethods.query.get(internal_execution_method)
        if not method:
            return jsonify({"message": f"Execution method '{internal_execution_method}' not found"}), 404
        return jsonify(method.json()), 200
    except Exception as e:
        return jsonify({"message": "Error retrieving execution method", "error": str(e)}), 500


@flask_app.route('/Update_ExecutionMethod/<string:internal_execution_method>', methods=['PUT'])
def Update_ExecutionMethod(internal_execution_method):
    try:
        execution_method = DefAsyncExecutionMethods.query.filter_by(internal_execution_method=internal_execution_method).first()

        if execution_method:
            # Only update fields that are provided in the request
            if 'execution_method' in request.json:
                execution_method.execution_method = request.json.get('execution_method')
            if 'executor' in request.json:
                execution_method.executor = request.json.get('executor')
            if 'description' in request.json:
                execution_method.description = request.json.get('description')

            execution_method.last_updated_by = 101

            # Update the last update timestamp
            execution_method.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({"message": "Execution method updated successfully"}), 200)

        return make_response(jsonify({"message": f"Execution method with internal_execution_method '{internal_execution_method}' not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error updating execution method", "error": str(e)}), 500)

# Create a task definition
@flask_app.route('/Create_Task', methods=['POST'])
def Create_Task():
    try:
        user_task_name = request.json.get('user_task_name')
        task_name = request.json.get('task_name')
        execution_method = request.json.get('execution_method')
        internal_execution_method = request.json.get('internal_execution_method')
        executor = request.json.get('executor')
        script_name = request.json.get('script_name')
        script_path = request.json.get('script_path')
        description = request.json.get('description')
        srs = request.json.get('srs')
        sf  = request.json.get('sf')

        new_task = DefAsyncTask(
            user_task_name = user_task_name,
            task_name = task_name,
            execution_method = execution_method,
            internal_execution_method = internal_execution_method,
            executor = executor,
            script_name = script_name,
            script_path = script_path,
            description = description,
            cancelled_yn = 'N',
            srs = srs,
            sf  = sf,
            created_by = 101
            #last_updated_by=last_updated_by

        )
        db.session.add(new_task)
        db.session.commit()

        return {"message": "DEF async task created successfully"}, 201

    except Exception as e:
        return {"message": "Error creating Task", "error": str(e)}, 500


@flask_app.route('/Show_Tasks', methods=['GET'])
def Show_Tasks():
    try:
        tasks = DefAsyncTask.query.all()
        return make_response(jsonify([task.json() for task in tasks]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting DEF async Tasks", "error": str(e)}), 500)


@flask_app.route('/Show_Task/<task_name>', methods=['GET'])
def Show_Task(task_name):
    try:
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()

        if not task:
            return {"message": f"Task with name '{task_name}' not found"}, 404

        return make_response(jsonify(task.json()), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error getting the task", "error": str(e)}), 500)


@flask_app.route('/Update_Task/<string:task_name>', methods=['PUT'])
def Update_Task(task_name):
    try:
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if task:
            # Only update fields that are provided in the request
            if 'user_task_name' in request.json:
                task.user_task_name = request.json.get('user_task_name')
            if 'execution_method' in request.json:
                task.execution_method = request.json.get('execution_method')
            if 'script_name' in request.json:
                task.script_name = request.json.get('script_name')
            if 'description' in request.json:
                task.description = request.json.get('description')
            if 'srs' in request.json:
                task.srs = request.json.get('srs')
            if 'sf' in request.json:
                task.sf = request.json.get('sf')
            if 'last_updated_by' in request.json:
                task.last_updated_by = request.json.get('last_updated_by')

            # Update the timestamps to reflect the modification time
            task.updated_at = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({"message": "DEF async Task updated successfully"}), 200)

        return make_response(jsonify({"message": f"DEF async Task with name '{task_name}' not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error updating DEF async Task", "error": str(e)}), 500)


@flask_app.route('/Cancel_Task/<string:task_name>', methods=['PUT'])
def Cancel_Task(task_name):
    try:
        # Find the task by task_name in the DEF_ASYNC_TASKS table
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()

        if task:
            # Update the cancelled_yn field to 'Y' (indicating cancellation)
            task.cancelled_yn = 'Y'

            db.session.commit()

            return make_response(jsonify({"message": f"Task {task_name} has been cancelled successfully"}), 200)

        return make_response(jsonify({"message": f"Task {task_name} not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error cancelling Task", "error": str(e)}), 500)



@flask_app.route('/Add_TaskParams/<string:task_name>', methods=['POST'])
def Add_TaskParams(task_name):
    try:
        # Check if the task exists in the DEF_ASYNC_TASKS table
        existing_task = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if not existing_task:
            return jsonify({"error": f"Task '{task_name}' does not exist"}), 404

        task_name = existing_task.task_name
        # Fetch parameters from the request
        parameters = request.json.get('parameters', [])
        if not parameters:
            return jsonify({"error": "No parameters provided"}), 400

        new_params = []
        for param in parameters:
            #seq = param.get('seq')
            parameter_name = param.get('parameter_name')
            data_type = param.get('data_type')
            description = param.get('description')
            created_by = request.json.get('created_by')

            # Validate required fields
            if not (parameter_name and data_type):
                return jsonify({"error": "Missing required parameter fields"}), 400

            # Create a new parameter object
            new_param = DefAsyncTaskParam(
                task_name=task_name,
                #seq=seq,
                parameter_name=parameter_name,
                data_type=data_type,
                description=description,
                created_by=created_by
            )
            new_params.append(new_param)

        # Add all new parameters to the session and commit
        db.session.add_all(new_params)
        db.session.commit()

        return make_response(jsonify([param.json() for param in new_params])), 200
    except Exception as e:
        return jsonify({"error": "Failed to create task parameters", "details": str(e)}), 500



@flask_app.route('/Show_TaskParams/<string:task_name>', methods=['GET'])
def Show_Parameter(task_name):
    try:
        parameters = DefAsyncTaskParam.query.filter_by(task_name=task_name).all()

        if not parameters:
            return make_response(jsonify({"message": f"No parameters found for task '{task_name}'"}), 404)

        return make_response(jsonify([param.json() for param in parameters]), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error getting Task Parameters", "error": str(e)}), 500)


@flask_app.route('/Update_TaskParams/<string:task_name>/<int:def_param_id>', methods=['PUT'])
def Update_TaskParams(task_name, def_param_id):
    try:
        # Get the updated values from the request body
        parameter_name = request.json.get('parameter_name')
        data_type = request.json.get('data_type')
        description = request.json.get('description')
        last_updated_by = 101

        # Find the task parameter by task_name and seq
        param = DefAsyncTaskParam.query.filter_by(task_name=task_name, def_param_id=def_param_id).first()

        # If the parameter does not exist, return a 404 response
        if not param:
            return jsonify({"message": f"Parameter with def_param_id '{def_param_id}' not found for task '{task_name}'"}), 404

        # Update the fields with the new values
        if parameter_name:
            param.parameter_name = parameter_name
        if data_type:
            param.data_type = data_type
        if description:
            param.description = description
        if last_updated_by:
            param.last_updated_by = last_updated_by

        # Commit the changes to the database
        db.session.commit()

        return jsonify({"message": "Task parameter updated successfully", "task_param": param.json()}), 200

    except Exception as e:
        return jsonify({"error": "Error updating task parameter", "details": str(e)}), 500



@flask_app.route('/Delete_TaskParams/<string:task_name>/<int:def_param_id>', methods=['DELETE'])
def Delete_TaskParams(task_name, def_param_id):
    try:
        # Find the task parameter by task_name and seq
        param = DefAsyncTaskParam.query.filter_by(task_name=task_name, def_param_id=def_param_id).first()

        # If the parameter does not exist, return a 404 response
        if not param:
            return jsonify({"message": f"Parameter with def_param_id '{def_param_id}' not found for task '{task_name}'"}), 404

        # Delete the parameter from the database
        db.session.delete(param)
        db.session.commit()

        return jsonify({"message": f"Parameter with def_param_id '{def_param_id}' successfully deleted from task '{task_name}'"}), 200

    except Exception as e:
        return jsonify({"error": "Failed to delete task parameter", "details": str(e)}), 500


@flask_app.route('/Delete_ExecutionMethod/<string:internal_execution_method>', methods=['DELETE'])
def Delete_ExecutionMethod(internal_execution_method):
    try:
        # Find the execution method by internal_execution_method
        execution_method = DefAsyncExecutionMethods.query.filter_by(internal_execution_method=internal_execution_method).first()

        # If the execution method does not exist, return a 404 response
        if not execution_method:
            return jsonify({"message": f"Execution method with internal_execution_method '{internal_execution_method}' not found"}), 404

        # Delete the execution method from the database
        db.session.delete(execution_method)
        db.session.commit()

        return jsonify({"message": f"Execution method with internal_execution_method '{internal_execution_method}' successfully deleted"}), 200

    except Exception as e:
        return jsonify({"error": "Failed to delete execution method", "details": str(e)}), 500



# @flask_app.route('/api/v1/Create_TaskSchedule', methods=['POST'])
# def Create_TaskSchedule_v1():
#     try:
#         user_schedule_name = request.json.get('user_schedule_name', 'Immediate')
#         task_name = request.json.get('task_name')
#         parameters = request.json.get('parameters', {})
#         schedule_type = request.json.get('schedule_type')
#         schedule_data = request.json.get('schedule', {})

#         if not task_name:
#             return jsonify({'error': 'Task name is required'}), 400

#         # Fetch task details from the database
#         task = DefAsyncTask.query.filter_by(task_name=task_name).first()
#         if not task:
#             return jsonify({'error': f'No task found with task_name: {task_name}'}), 400

#         user_task_name = task.user_task_name
#         executor = task.executor
#         script_name = task.script_name

#         schedule_name = str(uuid.uuid4())
#         redbeat_schedule_name = f"{user_schedule_name}_{schedule_name}"
#         args = [script_name, user_task_name, task_name, user_schedule_name, redbeat_schedule_name, schedule_data]
#         kwargs = {}

#         # Validate task parameters
#         task_params = DefAsyncTaskParam.query.filter_by(task_name=task_name).all()
#         for param in task_params:
#             param_name = param.parameter_name
#             if param_name in parameters:
#                 kwargs[param_name] = parameters[param_name]
#             else:
#                 return jsonify({'error': f'Missing value for parameter: {param_name}'}), 400

#         # Handle scheduling based on schedule type
#         cron_schedule = None
#         schedule_minutes = None

#         if schedule_type == "WEEKLY_SPECIFIC_DAYS":
#             values = schedule_data.get('VALUES', [])  # e.g., ["Monday", "Wednesday"]
#             day_map = {
#                 "SUN": 0, "MON": 1, "TUE": 2, "WED": 3,
#                 "THU": 4, "FRI": 5, "SAT": 6
#             }
#             days_of_week = ",".join(str(day_map[day.upper()]) for day in values if day.upper() in day_map)
#             cron_schedule = crontab(minute=0, hour=0, day_of_week=days_of_week)

#         elif schedule_type == "MONTHLY_SPECIFIC_DATES":
#             values = schedule_data.get('VALUES', [])  # e.g., ["5", "15"]
#             dates_of_month = ",".join(values)
#             cron_schedule = crontab(minute=0, hour=0, day_of_month=dates_of_month)

#         elif schedule_type == "ONCE":
#             one_time_date = schedule_data.get('VALUES')  # e.g., {"date": "2025-03-01 14:30"}
#             if not one_time_date:
#                 return jsonify({'error': 'Date is required for one-time execution'}), 400
#             dt = datetime.strptime(one_time_date, "%Y-%m-%d %H:%M")
#             cron_schedule = crontab(minute=dt.minute, hour=dt.hour, day_of_month=dt.day, month_of_year=dt.month)

#         elif schedule_type == "PERIODIC":
#             frequency_type = schedule_data.get('FREQUENCY_TYPE', 'minutes').lower()
#             frequency = schedule_data.get('FREQUENCY', 1)

#             if frequency_type == 'month':
#                 schedule_minutes = frequency * 30 * 24 * 60
#             elif frequency_type == 'day':
#                 schedule_minutes = frequency * 24 * 60
#             elif frequency_type == 'hour':
#                 schedule_minutes = frequency * 60
#             else:
#                 schedule_minutes = frequency  # Default to minutes

#         # Handle Ad-hoc Requests
#         elif schedule_type == "IMMEDIATE":
#             try:
#                 result = execute_ad_hoc_task_v1(
#                     user_schedule_name=user_schedule_name,
#                     executor=executor,
#                     task_name=task_name,
#                     args=args,
#                     kwargs=kwargs,
#                     schedule_type=schedule_type,
#                     cancelled_yn='N',
#                     created_by=101
#                 )
#                 return jsonify(result), 201
#             except Exception as e:
#                 return jsonify({"error": "Failed to execute ad-hoc task", "details": str(e)}), 500

#         else:
#             return jsonify({'error': 'Invalid schedule type'}), 400
#         # Handle Scheduled Tasks
#         try:
#             create_redbeat_schedule(
#                 schedule_name=redbeat_schedule_name,
#                 executor=executor,
#                 schedule_minutes=schedule_minutes if schedule_minutes else None,
#                 cron_schedule=cron_schedule if cron_schedule else None,
#                 args=args,
#                 kwargs=kwargs,
#                 celery_app=celery
#             )
#         except Exception as e:
#             return jsonify({"error": "Failed to create RedBeat schedule", "details": str(e)}), 500

#         # Store schedule in DB
#         new_schedule = DefAsyncTaskScheduleNew(
#             user_schedule_name=user_schedule_name,
#             redbeat_schedule_name=redbeat_schedule_name,
#             task_name=task_name,
#             args=args,
#             kwargs=kwargs,
#             parameters=kwargs,
#             schedule_type=schedule_type,
#             schedule=schedule_data,
#             ready_for_redbeat="N",
#             cancelled_yn='N',
#             created_by=101
#         )

#         db.session.add(new_schedule)
#         db.session.commit()

#         return jsonify({
#             "message": "Task schedule created successfully!",
#             "schedule_id": new_schedule.def_task_sche_id
#         }), 201

#     except Exception as e:
#         db.session.rollback()
#         return jsonify({"error": "Failed to create task schedule", "details": str(e)}), 500


@flask_app.route('/Create_TaskSchedule', methods=['POST'])
def Create_TaskSchedule():
    try:
        user_schedule_name = request.json.get('user_schedule_name', 'Immediate')
        task_name = request.json.get('task_name')
        parameters = request.json.get('parameters', {})
        schedule_type = request.json.get('schedule_type')
        schedule_data = request.json.get('schedule', {})

        if not task_name:
            return jsonify({'error': 'Task name is required'}), 400

        # Fetch task details from the database
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if not task:
            return jsonify({'error': f'No task found with task_name: {task_name}'}), 400

        user_task_name = task.user_task_name
        executor = task.executor
        script_name = task.script_name

        schedule_name = str(uuid.uuid4())
        redbeat_schedule_name = f"{user_schedule_name}_{schedule_name}"
        args = [script_name, user_task_name, task_name, user_schedule_name, redbeat_schedule_name, schedule_type, schedule_data]
        kwargs = {}

        # Validate task parameters
        task_params = DefAsyncTaskParam.query.filter_by(task_name=task_name).all()
        for param in task_params:
            param_name = param.parameter_name
            if param_name in parameters:
                kwargs[param_name] = parameters[param_name]
            else:
                return jsonify({'error': f'Missing value for parameter: {param_name}'}), 400

        # Handle scheduling based on schedule type
        cron_schedule = None
        schedule_minutes = None

        if schedule_type == "WEEKLY_SPECIFIC_DAYS":
            values = schedule_data.get('VALUES', [])  # e.g., ["Monday", "Wednesday"]
            day_map = {
                "SUN": 0, "MON": 1, "TUE": 2, "WED": 3,
                "THU": 4, "FRI": 5, "SAT": 6
            }
            days_of_week = ",".join(str(day_map[day.upper()]) for day in values if day.upper() in day_map)
            cron_schedule = crontab(minute=0, hour=0, day_of_week=days_of_week)

        elif schedule_type == "MONTHLY_SPECIFIC_DATES":
            values = schedule_data.get('VALUES', [])  # e.g., ["5", "15"]
            dates_of_month = ",".join(values)
            cron_schedule = crontab(minute=0, hour=0, day_of_month=dates_of_month)

        elif schedule_type == "ONCE":
            one_time_date = schedule_data.get('VALUES')  # e.g., {"date": "2025-03-01 14:30"}
            if not one_time_date:
                return jsonify({'error': 'Date is required for one-time execution'}), 400
            dt = datetime.strptime(one_time_date, "%Y-%m-%d %H:%M")
            cron_schedule = crontab(minute=dt.minute, hour=dt.hour, day_of_month=dt.day, month_of_year=dt.month)

        elif schedule_type == "PERIODIC":
            # Extract frequency type and frequency value from schedule_data
            frequency_type_raw = schedule_data.get('FREQUENCY_TYPE', 'MINUTES')
            frequency_type = frequency_type_raw.upper().strip().rstrip('s').replace('(', '').replace(')', '')
            frequency = schedule_data.get('FREQUENCY', 1)

            # Log frequency values to help with debugging
            print(f"Frequency Type: {frequency_type}")
            print(f"Frequency: {frequency}")
           
            # Handle different frequency types
            if frequency_type == 'MONTHS':
               schedule_minutes = frequency * 30 * 24 * 60  # Approximate calculation: 1 month = 30 days
            elif frequency_type == 'WEEKS':
               schedule_minutes = frequency * 7 * 24 * 60  # 7 days * 24 hours * 60 minutes
            elif frequency_type == 'DAYS':
               schedule_minutes = frequency * 24 * 60  # 1 day = 24 hours = 1440 minutes
            elif frequency_type == 'HOURS':
               schedule_minutes = frequency * 60  # 1 hour = 60 minutes
            elif frequency_type == 'MINUTES':
               schedule_minutes = frequency  # Frequency is already in minutes
            else:
               return jsonify({'error': f'Invalid frequency type: {frequency_type}'}), 400

        # Handle Ad-hoc Requests
        elif schedule_type == "IMMEDIATE":
            try:
                result = execute_ad_hoc_task_v1(
                    user_schedule_name=user_schedule_name,
                    executor=executor,
                    task_name=task_name,
                    args=args,
                    kwargs=kwargs,
                    schedule_type=schedule_type,
                    cancelled_yn='N',
                    created_by=101
                )
                return jsonify(result), 201
            except Exception as e:
                return jsonify({"error": "Failed to execute ad-hoc task", "details": str(e)}), 500

        else:
            return jsonify({'error': 'Invalid schedule type'}), 400
        # Handle Scheduled Tasks
        try:
            create_redbeat_schedule(
                schedule_name=redbeat_schedule_name,
                executor=executor,
                schedule_minutes=schedule_minutes if schedule_minutes else None,
                cron_schedule=cron_schedule if cron_schedule else None,
                args=args,
                kwargs=kwargs,
                celery_app=celery
            )
        except Exception as e:
            return jsonify({"error": "Failed to create RedBeat schedule", "details": str(e)}), 500

        # Store schedule in DB
        new_schedule = DefAsyncTaskScheduleNew(
            user_schedule_name=user_schedule_name,
            redbeat_schedule_name=redbeat_schedule_name,
            task_name=task_name,
            args=args,
            kwargs=kwargs,
            parameters=kwargs,
            schedule_type=schedule_type,
            schedule=schedule_data,
            ready_for_redbeat="N",
            cancelled_yn='N',
            created_by=101
        )

        db.session.add(new_schedule)
        db.session.commit()

        return jsonify({
            "message": "Task schedule created successfully!",
            "schedule_id": new_schedule.def_task_sche_id
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": "Failed to create task schedule", "details": str(e)}), 500


@flask_app.route('/Show_TaskSchedules', methods=['GET'])
def Show_TaskSchedules():
    try:
        schedules = DefAsyncTaskSchedulesV.query.filter( DefAsyncTaskSchedulesV.ready_for_redbeat != 'Y').all()
        # Return the schedules as a JSON response
        return jsonify([schedule.json() for schedule in schedules])

    except Exception as e:
        # Handle any errors and return them as a JSON response
        return jsonify({"error": str(e)}), 500


@flask_app.route('/Show_TaskSchedule/<string:task_name>', methods=['GET'])
def Show_TaskSchedule(task_name):
    try:
        schedule = DefAsyncTaskSchedule.query.filter_by(task_name=task_name).first()
        if schedule:
            return make_response(jsonify(schedule.json()), 200)

        return make_response(jsonify({"message": f"Task Periodic Schedule for {task_name} not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving Task Periodic Schedule", "error": str(e)}), 500)



# @flask_app.route('/Update_TaskSchedule/<string:task_name>', methods=['PUT'])
# def Update_TaskSchedule(task_name):

#     try:
#         # Retrieve redbeat_schedule_name from request payload
#         redbeat_schedule_name = request.json.get('redbeat_schedule_name')
#         if not redbeat_schedule_name:
#             return make_response(jsonify({"message": "redbeat_schedule_name is required in the payload"}), 400)

#         # Retrieve the schedule from the database
#         schedule = DefAsyncTaskScheduleNew.query.filter_by(
#             task_name=task_name, redbeat_schedule_name=redbeat_schedule_name
#         ).first()

#         # Check if schedule exists
#         if not schedule:
#             return make_response(jsonify({"message": f"Task Periodic Schedule for {redbeat_schedule_name} not found"}), 404)

#         # Check if ready_for_redbeat is 'N' (allow updates only if it's 'N')
#         if schedule.ready_for_redbeat != 'N':
#             return make_response(jsonify({
#                 "message": f"Task Periodic Schedule for {redbeat_schedule_name} is not marked as 'N'. Update is not allowed."
#             }), 400)

#         # Update database fields based on the request data
#         if 'parameters' in request.json:
#             schedule.parameters = request.json.get('parameters')
#             schedule.kwargs = request.json.get('parameters')
#         if 'schedule_type' in request.json:
#             schedule.schedule_type = request.json.get('schedule_type')
#         if 'schedule' in request.json:
#             schedule.schedule = request.json.get('schedule')

#         schedule.last_updated_by = 102  # Static user ID

#         # Commit changes to the database
#         db.session.commit()

#         return make_response(jsonify({
#             "message": f"Task Periodic Schedule for {redbeat_schedule_name} updated successfully in the database"
#         }), 200)

#     except Exception as e:
#         db.session.rollback()  # Rollback in case of an error
#         return make_response(jsonify({"message": "Error updating Task Periodic Schedule", "error": str(e)}), 500)


@flask_app.route('/Update_TaskSchedule/<string:task_name>', methods=['PUT'])
def Update_TaskSchedule(task_name):
    try:
        redbeat_schedule_name = request.json.get('redbeat_schedule_name')
        if not redbeat_schedule_name:
            return jsonify({"message": "redbeat_schedule_name is required in the payload"}), 400

        schedule = DefAsyncTaskScheduleNew.query.filter_by(
            task_name=task_name, redbeat_schedule_name=redbeat_schedule_name
        ).first()
        executors = DefAsyncTask.query.filter_by(task_name=task_name).first()

        if not schedule:
            return jsonify({"message": f"Task Periodic Schedule for {redbeat_schedule_name} not found"}), 404

        if schedule.ready_for_redbeat != 'N':
            return jsonify({
                "message": f"Task Periodic Schedule for {redbeat_schedule_name} is not marked as 'N'. Update is not allowed."
            }), 400

        # Update fields
        schedule.parameters = request.json.get('parameters', schedule.parameters)
        schedule.kwargs = request.json.get('parameters', schedule.kwargs)
        schedule.schedule_type = request.json.get('schedule_type', schedule.schedule_type)
        schedule.schedule = request.json.get('schedule', schedule.schedule)
        schedule.last_updated_by = 102  # Static user ID

        # Handle scheduling logic
        cron_schedule = None
        schedule_minutes = None

        if schedule.schedule_type == "WEEKLY_SPECIFIC_DAYS":
            values = schedule.schedule.get('VALUES', [])
            day_map = {"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6}
            days_of_week = ",".join(str(day_map[day.upper()]) for day in values if day.upper() in day_map)
            cron_schedule = crontab(minute=0, hour=0, day_of_week=days_of_week)

        elif schedule.schedule_type == "MONTHLY_SPECIFIC_DATES":
            values = schedule.schedule.get('VALUES', [])
            dates_of_month = ",".join(values)
            cron_schedule = crontab(minute=0, hour=0, day_of_month=dates_of_month)

        elif schedule.schedule_type == "ONCE":
            one_time_date = schedule.schedule.get('VALUES')
            dt = datetime.strptime(one_time_date, "%Y-%m-%d %H:%M")
            cron_schedule = crontab(minute=dt.minute, hour=dt.hour, day_of_month=dt.day, month_of_year=dt.month)

        elif schedule.schedule_type == "PERIODIC":
            frequency_type = schedule.schedule.get('FREQUENCY_TYPE', 'minutes').lower()
            frequency = schedule.schedule.get('FREQUENCY', 1)
            
            if frequency_type == 'months':
                schedule_minutes = frequency * 30 * 24 * 60
            elif frequency_type == 'days':
                schedule_minutes = frequency * 24 * 60
            elif frequency_type == 'hours':
                schedule_minutes = frequency * 60
            else:
                schedule_minutes = frequency  # Default to minutes

        # Ensure at least one scheduling method is provided
        if not schedule_minutes and not cron_schedule:
            return jsonify({"message": "Either 'schedule_minutes' or 'cron_schedule' must be provided."}), 400

        # Update RedBeat schedule
        try:
            update_redbeat_schedule(
                schedule_name=redbeat_schedule_name,
                task=executors.executor,
                schedule_minutes=schedule_minutes,
                cron_schedule=cron_schedule,
                args=schedule.args,
                kwargs=schedule.kwargs,
                celery_app=celery
            )
        except Exception as e:
            db.session.rollback()
            return jsonify({"message": "Error updating Redis. Database changes rolled back.", "error": str(e)}), 500

        db.session.commit()
        return jsonify({"message": f"Task Schedule for {redbeat_schedule_name} updated successfully in database and Redis"}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error updating Task Schedule", "error": str(e)}), 500


@flask_app.route('/Cancel_TaskSchedule/<string:task_name>', methods=['PUT'])
def Cancel_TaskSchedule(task_name):
    try:
        # Extract redbeat_schedule_name from payload
        redbeat_schedule_name = request.json.get('redbeat_schedule_name')
        if not redbeat_schedule_name:
            return make_response(jsonify({"message": "redbeat_schedule_name is required in the payload"}), 400)

        # Find the task schedule in the database
        schedule = DefAsyncTaskScheduleNew.query.filter_by(task_name=task_name, redbeat_schedule_name=redbeat_schedule_name).first()

        if not schedule:
            return make_response(jsonify({"message": f"Task periodic schedule for {redbeat_schedule_name} not found"}), 404)

        # Check if ready_for_redbeat is 'N' (only then cancellation is allowed)
        if schedule.ready_for_redbeat != 'N':
            return make_response(jsonify({"message": f"Cancellation not allowed. Task periodic schedule for {redbeat_schedule_name} is already processed in Redis"}), 400)

        # Update the `cancelled_yn` field to 'Y' (marking it as cancelled)
        schedule.cancelled_yn = 'Y'

        # Commit the change to the database
        db.session.commit()

        # Now, call the function to delete the schedule from Redis
        redis_response, redis_status = delete_schedule_from_redis(redbeat_schedule_name)

        # If there is an issue deleting from Redis, rollback the database update
        if redis_status != 200:
            db.session.rollback()
            return make_response(jsonify({"message": "Task schedule cancelled, but failed to delete from Redis", "error": redis_response['error']}), 500)

        # Return success message if both operations are successful
        return make_response(jsonify({"message": f"Task periodic schedule for {redbeat_schedule_name} has been cancelled successfully in the database and deleted from Redis"}), 200)

    except Exception as e:
        db.session.rollback()  # Rollback on failure
        return make_response(jsonify({"message": "Error cancelling task periodic schedule", "error": str(e)}), 500)






@flask_app.route('/Cancel_AdHoc_Task/<string:task_name>/<string:user_schedule_name>/<string:schedule_id>/<string:task_id>', methods=['PUT'])
def Cancel_AdHoc_Task(task_name, user_schedule_name, schedule_id, task_id):
    """
    Cancels an ad-hoc task by updating the database and revoking the Celery task.

    Args:
        task_name (str): The name of the Celery task.
        user_schedule_name (str): The name of the user schedule.
        schedule_id (str): The database schedule ID.
        task_id (str): The Celery task ID.

    Returns:
        JSON response indicating success or failure.
    """
    try:
        # Find the task schedule by schedule_id and user_schedule_name
        schedule = DefAsyncTaskSchedule.query.filter_by(
            def_task_sche_id=schedule_id,
            user_schedule_name=user_schedule_name,
            task_name=task_name
        ).first()

        if schedule:
            # Update the cancelled_yn field to 'Y' (indicating cancellation)
            schedule.cancelled_yn = 'Y'

            # Commit the change to the database
            db.session.commit()

            # Now, revoke the Celery task
            try:
                celery.control.revoke(task_id, terminate=True)
                logging.info(f"Ad-hoc task with ID '{task_id}' revoked successfully.")
            except Exception as e:
                db.session.rollback()
                return make_response(jsonify({
                    "message": "Task schedule cancelled, but failed to revoke Celery task.",
                    "error": str(e)
                }), 500)

            # Return success message if both operations are successful
            return make_response(jsonify({
                "message": f"Ad-hoc task for schedule_id {schedule_id} has been successfully cancelled and revoked."
            }), 200)

        # If no schedule was found
        return make_response(jsonify({
            "message": f"No ad-hoc task found for schedule_id {schedule_id} and user_schedule_name {user_schedule_name}."
        }), 404)

    except Exception as e:
        db.session.rollback()
        logging.error(f"Error cancelling ad-hoc task: {str(e)}")
        return make_response(jsonify({
            "message": "Error cancelling ad-hoc task.",
            "error": str(e)
        }), 500)

    finally:
        db.session.close()


# @flask_app.route('/view_requests', methods=['GET'])
# def get_all_tasks():
#     try:
#         fourteen_days = datetime.utcnow() - timedelta(days=14)
#         tasks = DefAsyncTaskRequest.query.filter(DefAsyncTaskRequest.creation_date>=fourteen_days).all()
#         #tasks = DefAsyncTaskRequest.query.limit(100000).all()
#         if not tasks:
#             return jsonify({"message": "No tasks found"}), 404
#         return jsonify([task.json() for task in tasks]), 200
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

@flask_app.route('/view_requests/<int:page>/<int:page_limit>', methods=['GET'])
def view_requests(page, page_limit):
    try:
        fourteen_days = datetime.utcnow() - timedelta(days=14)
        # Filter by date
        query = DefAsyncTaskRequest.query.filter(
            DefAsyncTaskRequest.creation_date >= fourteen_days
        )
        
        # Total number of matching tasks
        total = query.count()
        total_pages = (total + page_limit - 1) // page_limit

        # Paginate using offset and limit
        requests = query.order_by(DefAsyncTaskRequest.creation_date.desc())\
                     .offset((page - 1) * page_limit).limit(page_limit) \
                     .all()

        if not requests:
            return jsonify({"message": "No tasks found"}), 404

        return jsonify({"total_records": total,
                        "total_pages": total_pages,
                        "requests": [request.json() for request in requests]
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



if __name__ == "__main__":
    flask_app.run(debug=True)
