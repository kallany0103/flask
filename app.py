#app.py
import os 
import time
import re
import json
import uuid
import requests
import traceback
import logging
import time
import re
from redis import Redis
from zoneinfo import ZoneInfo
from itertools import count
from functools import wraps
from flask_cors import CORS 
from dotenv import load_dotenv            # To load environment variables from a .env file
from celery.schedules import crontab
from celery.result import AsyncResult      # For checking the status of tasks
from redbeat import RedBeatSchedulerEntry
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine, Text, desc, cast, TIMESTAMP, func, or_
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, make_response       # Flask utilities for handling requests and responses
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from werkzeug.security import generate_password_hash, check_password_hash
from executors import flask_app # Import Flask app and tasks
from executors.extensions import db
from celery import current_app as celery  # Access the current Celery app
from executors.models import (
    DefAsyncTask,
    DefAsyncTaskParam,
    DefAsyncTaskSchedule,
    DefAsyncTaskRequest,
    DefAsyncTaskSchedulesV,
    DefAsyncExecutionMethods,
    DefAsyncTaskScheduleNew,
    DefTenant,
    DefUser,
    DefPerson,
    DefUserCredential,
    DefAccessProfile,
    DefUsersView,
    Message,
    DefTenantEnterpriseSetup,
    DefTenantEnterpriseSetupV,
    DefAccessModel,
    DefAccessModelLogic,
    DefAccessModelLogicAttribute,
    DefGlobalCondition,
    DefGlobalConditionLogic,
    DefGlobalConditionLogicAttribute,
    DefAccessPointElement,
    DefDataSource,
    DefAccessEntitlement,
    DefControl
)
from redbeat_s.red_functions import create_redbeat_schedule, update_redbeat_schedule, delete_schedule_from_redis
from ad_hoc.ad_hoc_functions import execute_ad_hoc_task, execute_ad_hoc_task_v1
from config import redis_url


redis_client = Redis.from_url(redis_url, decode_responses=True)

jwt = JWTManager(flask_app)
CORS(
    flask_app,
    resources={r"/*": {"origins": "http://localhost:5173"}},
    supports_credentials=True,
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["Content-Type", "Authorization"],
    methods=["GET", "POST", "PUT", "DELETE"]
)
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


# Create a tenant
@flask_app.route('/tenants', methods=['POST'])
@jwt_required()
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
@jwt_required()
def get_tenants():
    try:
        tenants = DefTenant.query.order_by(DefTenant.tenant_id.desc()).all()
        return make_response(jsonify([tenant.json() for tenant in tenants]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Tenants", "error": str(e)}), 500)

@flask_app.route('/tenants/v1', methods=['GET'])
def get_tenants_v1():
    try:
        tenants = DefTenant.query.order_by(DefTenant.tenant_id.desc()).all()
        return make_response(jsonify([tenant.json() for tenant in tenants]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Tenants", "error": str(e)}), 500)



@flask_app.route('/def_tenants/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_tenants(page, limit):
    try:
        query = DefTenant.query.order_by(DefTenant.tenant_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [tenant.json() for tenant in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error fetching paginated tenants", "error": str(e)}), 500)


@flask_app.route('/def_tenants/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_tenants(page, limit):
    try:
        search_query = request.args.get('tenant_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefTenant.query

        if search_query:
            query = query.filter(
                or_(
                    DefTenant.tenant_name.ilike(f'%{search_query}%'),
                    DefTenant.tenant_name.ilike(f'%{search_underscore}%'),
                    DefTenant.tenant_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefTenant.tenant_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [tenant.json() for tenant in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": 1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching tenants", "error": str(e)}), 500)



@flask_app.route('/tenants/<int:tenant_id>', methods=['GET'])
@jwt_required()
def get_tenant(tenant_id):
    try:
        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if tenant:
            return make_response(jsonify(tenant.json()),200)
        else:
            return make_response(jsonify({"message": "Tenant not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving tenant", "error": str(e)}), 500)


# Update a tenant
@flask_app.route('/tenants/<int:tenant_id>', methods=['PUT'])
@jwt_required()
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
@jwt_required()
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


# Create enterprise setup
@flask_app.route('/create_enterpriseV1/<int:tenant_id>', methods=['POST'])
@jwt_required()
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

# Create or update enterprise setup
@flask_app.route('/create_enterprise/<int:tenant_id>', methods=['POST'])
@jwt_required()
def create_update_enterprise(tenant_id):
    try:
        data = request.get_json()
        tenant_id       = tenant_id
        enterprise_name = data['enterprise_name']
        enterprise_type = data['enterprise_type']

        existing_enterprise = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()

        if existing_enterprise:
            existing_enterprise.enterprise_name = enterprise_name
            existing_enterprise.enterprise_type = enterprise_type
            message = "Enterprise setup updated successfully"

        else:
            new_enterprise = DefTenantEnterpriseSetup(
                tenant_id=tenant_id,
                enterprise_name=enterprise_name,
                enterprise_type=enterprise_type
            )

            db.session.add(new_enterprise)
            message = "Enterprise setup created successfully"

        db.session.commit()
        return make_response(jsonify({"message": message}), 200)

    except IntegrityError:
        return make_response(jsonify({"message": "Error creating or updating enterprise setup", "error": "Integrity error"}), 409)
    except Exception as e:
        return make_response(jsonify({"message": "Error creating or updating enterprise setup", "error": str(e)}), 500)



#Get all enterprise setups
@flask_app.route('/get_enterprises', methods=['GET'])
@jwt_required()
def get_enterprises():
    try:
        setups = DefTenantEnterpriseSetup.query.order_by(DefTenantEnterpriseSetup.tenant_id.desc()).all()
        return make_response(jsonify([setup.json() for setup in setups]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving enterprise setups", "error": str(e)}), 500)

@flask_app.route('/get_enterprises/v1', methods=['GET'])
def get_enterprises_v1():
    try:
        setups = DefTenantEnterpriseSetup.query.order_by(DefTenantEnterpriseSetup.tenant_id.desc()).all()
        return make_response(jsonify([setup.json() for setup in setups]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving enterprise setups", "error": str(e)}), 500)


# Get one enterprise setup by tenant_id
@flask_app.route('/get_enterprise/<int:tenant_id>', methods=['GET'])
@jwt_required()
def get_enterprise(tenant_id):
    try:
        setup = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()
        if setup:
            return make_response(jsonify(setup.json()), 200)
        return make_response(jsonify({"message": "Enterprise setup not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving enterprise setup", "error": str(e)}), 500)


@flask_app.route('/def_tenant_enterprise_setup/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_enterprises(page, limit):
    try:
        query = db.session.query(DefTenantEnterpriseSetupV).order_by(DefTenantEnterpriseSetupV.tenant_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return jsonify({
            "items": [row.json() for row in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200
    except Exception as e:
        return jsonify({"message": "Error fetching paginated enterprises", "error": str(e)}), 500

# Update enterprise setup
@flask_app.route('/update_enterprise/<int:tenant_id>', methods=['PUT'])
@jwt_required()
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
@jwt_required()
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

 

 



#get all tenants enterprise setups
@flask_app.route('/enterprises', methods=['GET'])
@jwt_required()
def enterprises():
    try:
        # results = db.session.query(DefTenantEnterpriseSetupV).all()
        results = db.session.query(DefTenantEnterpriseSetupV).order_by(
            DefTenantEnterpriseSetupV.tenant_id.desc()
        ).all()
        
        if not results:
            return jsonify({'data': [], 'message': 'No records found'}), 200

        data = [row.json() for row in results]
        
        return jsonify(data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@flask_app.route('/def_tenant_enterprise_setup/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_enterprises(page, limit):
    try:
        search_query = request.args.get('enterprise_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = db.session.query(DefTenantEnterpriseSetupV)

        if search_query:
            query = query.filter(
                or_(
                    DefTenantEnterpriseSetupV.enterprise_name.ilike(f'%{search_query}%'),
                    DefTenantEnterpriseSetupV.enterprise_name.ilike(f'%{search_underscore}%'),
                    DefTenantEnterpriseSetupV.enterprise_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefTenantEnterpriseSetupV.tenant_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [row.json() for row in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching enterprises", "error": str(e)}), 500)


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
        profile_picture = data.get('profile_picture') or {
            "original": "uploads/profiles/default/profile.jpg",
            "thumbnail": "uploads/profiles/default/thumbnail.jpg"
        }
        

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
          tenant_id       = tenant_id,
          profile_picture = profile_picture
        )
        # Add the new user to the database session
        db.session.add(new_user)
        # Commit the changes to the database
        db.session.commit()

        # Return a success response
        return make_response(jsonify({"message": "Def USER created successfully!",
                                       "User Id": user_id}), 201)

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    


@flask_app.route('/defusers', methods=['GET'])
def get_users():
    try:
        users = DefUser.query.all()
        return make_response(jsonify([user.json() for user in users]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'error getting users', 'error': str(e)}), 500)
    

@flask_app.route('/defusers/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_def_users(page, limit):
    try:
        query = DefUser.query.order_by(DefUser.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting paginated users', 'error': str(e)}), 500)



@flask_app.route('/defusers/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_users(page, limit):
    try:
        search_query = request.args.get('user_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefUser.query

        if search_query:
            query = query.filter(
                or_(
                    DefUser.user_name.ilike(f'%{search_query}%'),
                    DefUser.user_name.ilike(f'%{search_underscore}%'),
                    DefUser.user_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefUser.user_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching users", "error": str(e)}), 500)


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



@flask_app.route('/def_combined_user/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_combined_users(page, limit):
    try:
        query = DefUsersView.query.order_by(DefUsersView.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching combined users', 'error': str(e)}), 500)
        

@flask_app.route('/def_combined_user/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_combined_users(page, limit):
    user_name = request.args.get('user_name', '').strip()
    try:
        query = DefUsersView.query
        if user_name:
            query = query.filter(DefUsersView.user_name.ilike(f'%{user_name}%'))
        query = query.order_by(DefUsersView.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching combined users', 'error': str(e)}), 500)


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
    



@flask_app.route('/defpersons/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_persons(page, limit):
    try:
        query = DefPerson.query.order_by(DefPerson.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [person.json() for person in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting paginated persons', 'error': str(e)}), 500)

@flask_app.route('/defpersons/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_persons(page, limit):
    try:
        search_query = request.args.get('name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefPerson.query

        if search_query:
            query = query.filter(
                or_(
                    func.lower(DefPerson.first_name).ilike(f'%{search_query}%'),
                    func.lower(DefPerson.first_name).ilike(f'%{search_underscore}%'),
                    func.lower(DefPerson.first_name).ilike(f'%{search_space}%'),
                    func.lower(DefPerson.last_name).ilike(f'%{search_query}%'),
                    func.lower(DefPerson.last_name).ilike(f'%{search_underscore}%'),
                    func.lower(DefPerson.last_name).ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefPerson.user_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [person.json() for person in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching persons", "error": str(e)}), 500)


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
  
    
# @flask_app.route('/users', methods=['POST'])
# #@jwt_required()
# #@role_required('/users', 'POST')
# def create_user():
#     try:
#         data = request.get_json()
#         user_id         = generate_user_id()
#         user_name       = data['user_name']
#         user_type       = data['user_type']
#         email_addresses = data['email_addresses']
#         first_name      = data['first_name']
#         middle_name     = data['middle_name']
#         last_name       = data['last_name']
#         job_title       = data['job_title']
#         # Get user information from the JWT token
#         #created_by      = get_jwt_identity()
#         #last_updated_by = get_jwt_identity()
#         created_by      = data['created_by']
#         last_updated_by = data['last_updated_by']
#         # created_on      = data['created_on']
#         # last_updated_on = data['last_updated_on']
#         tenant_id       = data['tenant_id']
#         password        = data['password']
#         # privilege_names = data.get('privilege_names', [])
#         # role_names      = data.get('role_names', [])

#         existing_user  = DefUser.query.filter(DefUser.user_name == user_name).first()
#         # existing_user = ArcPerson.query.filter((ArcPerson.user_id == user_id) | (ArcPerson.username == username)).first()
#         existing_email = DefUser.query.filter(DefUser.email_addresses == email_addresses).first()

#         if existing_user:
#             if existing_user.user_name == user_name:
#                 return make_response(jsonify({"message": "Username already exists"}), 400)
        
#         if existing_email:
#             return make_response(jsonify({"message": "Email address already exists"}), 409)
            
#         hashed_password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)
        
#         #current_timestamp = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
#         # Create a user record for the new user
#         def_user = DefUser(
#             user_id         = user_id,
#             user_name       = user_name,
#             user_type       = user_type,
#             email_addresses = email_addresses,
#             created_by      = created_by,
#             created_on      = current_timestamp(),
#             last_updated_by = last_updated_by,
#             last_updated_on = current_timestamp(),
#             tenant_id       = tenant_id
            
#         )

#         # Add the person record for the new user
#         db.session.add(def_user)
#         db.session.commit()

#         # Check if user_type is "person" before creating ArcPerson record
#         if user_type.lower() == "person":
#             def_person_data = {
#                 "user_id"    : user_id,
#                 "first_name" : first_name,
#                 "middle_name": middle_name,
#                 "last_name"  : last_name,
#                 "job_title"  : job_title
#             }

#             def_person_response = requests.post("http://localhost:5000/defpersons", json=def_person_data)

#             # Check the response from create_arc_user API
#             if def_person_response.status_code != 201:
#                 return jsonify({"message": "Def person's record creation failed"}), 500

    
#         # Create user credentials
#         user_credentials_data = {
#             "user_id" : user_id,
#             "password": hashed_password
#         }
#         user_credentials_response = requests.post("http://localhost:5000/def_user_credentials", json=user_credentials_data)

#         if user_credentials_response.status_code != 201:
#             # Handle the case where user credentials creation failed
#             return make_response(jsonify({"message": "User credentials creation failed"}), 500)


#     #    # Assign privileges to the user
#     #     user_privileges_data = {
#     #         "user_id": user_id,
#     #         "privilege_names": privilege_names
#     #     }
#     #     user_privileges_response = requests.post("http://localhost:5000/user_granted_privileges", json=user_privileges_data)

#     #     if user_privileges_response.status_code != 201:
#     #         return jsonify({'message': 'User privileges assignment failed'}), 500

#     #     # Assign roles to the user
#     #     user_roles_data = {
#     #         "user_id": user_id,
#     #         "role_names": role_names
#     #     }
#     #     user_roles_response = requests.post("http://localhost:5000/user_granted_roles", json=user_roles_data)

#     #     if user_roles_response.status_code != 201:
#     #         return jsonify({'message': 'User roles assignment failed'}), 500

#     #     db.session.commit()

#         return jsonify({"message": "User created successfully"}), 201
    
#     except IntegrityError as e:
#         return jsonify({"message": "User id already exists"}), 400  # 400 Bad Request for unique constraint violation
   
#     except Exception as e:
#         return jsonify({"message": str(e)}), 500
#         traceback.print_exc()




@flask_app.route('/users', methods=['POST'])
def register_user():
    try:
        data = request.get_json()
        # Extract user fields
        user_id         = generate_user_id()
        user_name       = data['user_name']
        user_type       = data['user_type']
        email_addresses = data['email_addresses']
        created_by      = data['created_by']
        last_updated_by = data['last_updated_by']
        tenant_id       = data['tenant_id']
        # Extract person fields
        first_name      = data.get('first_name')
        middle_name     = data.get('middle_name')
        last_name       = data.get('last_name')
        job_title       = data.get('job_title')
        # Extract credentials
        password        = data['password']

        # Set default profile picture if not provided
        profile_picture = data.get('profile_picture') or {
            "original": "uploads/profiles/default/profile.jpg",
            "thumbnail": "uploads/profiles/default/thumbnail.jpg"
        }

        # Check for existing user/email
        if DefUser.query.filter_by(user_name=user_name).first():
            return jsonify({"message": "Username already exists"}), 409
        for email in email_addresses:
            if DefUser.query.filter(DefUser.email_addresses.contains    ([email])).first():
                return jsonify({"message": "Email already exists"}), 409

        # Create user
        new_user = DefUser(
            user_id         = user_id,
            user_name       = user_name,
            user_type       = user_type,
            email_addresses = email_addresses,
            created_by      = created_by,
            created_on      = current_timestamp(),
            last_updated_by = last_updated_by,
            last_updated_on = current_timestamp(),
            tenant_id       = tenant_id,
            profile_picture = profile_picture
        )
        db.session.add(new_user)

        # Create person if user_type is person
        if user_type.lower() == "person":
            new_person = DefPerson(
                user_id     = user_id,
                first_name  = first_name,
                middle_name = middle_name,
                last_name   = last_name,
                job_title   = job_title
            )
            db.session.add(new_person)

        # Create credentials
        hashed_password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)
        new_cred = DefUserCredential(
            user_id  = user_id,
            password = hashed_password
        )
        db.session.add(new_cred)

        db.session.commit()
        return jsonify({"message": "User registered successfully", "user_id": user_id}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Registration failed", "error": str(e)}), 500

@flask_app.route('/users', methods=['GET'])
# @jwt_required()
def defusers():
    try:
        defusers = DefUsersView.query.order_by(DefUsersView.user_id.desc()).all()
        return make_response(jsonify([defuser.json() for defuser in defusers]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting users', 'error': str(e)}), 500)
    
    
@flask_app.route('/users/<int:user_id>', methods=['GET'])
# @jwt_required()
def get_specific_user(user_id):
    try:
        user = DefUsersView.query.filter_by(user_id=user_id).first()
        if user:
            return make_response(jsonify({'user': user.json()}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting User', 'error': str(e)}), 500)  
    

@flask_app.route('/users/<int:user_id>', methods=['PUT'])
def update_specific_user(user_id):
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({'message': 'No input data provided'}), 400)

        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({'message': 'User not found'}), 404)

        # Update DefUser fields
        user.user_name = data.get('user_name', user.user_name)
        user.email_addresses = data.get('email_addresses', user.email_addresses)
        user.last_updated_on = current_timestamp()

        # Update DefPerson fields if user_type is "person"
        if user.user_type and user.user_type.lower() == "person":
            person = DefPerson.query.filter_by(user_id=user_id).first()
            if not person:
                return make_response(jsonify({'message': 'Person not found'}), 404)

            person.first_name = data.get('first_name', person.first_name)
            person.middle_name = data.get('middle_name', person.middle_name)
            person.last_name = data.get('last_name', person.last_name)
            person.job_title = data.get('job_title', person.job_title)

        # Password update logic
        password = data.get('password')
        if password:
            user_cred = DefUserCredential.query.filter_by(user_id=user_id).first()
            if not user_cred:
                return make_response(jsonify({'message': 'User credentials not found'}), 404)

            user_cred.password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)

        db.session.commit()
        return make_response(jsonify({'message': 'User updated successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error updating user', 'error': str(e)}), 500)



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


# @flask_app.route('/users/<int:user_id>', methods=['DELETE'])
# def delete_specific_user(user_id):
#     try:
#         user = DefUser.query.filter_by(user_id=user_id).first()
#         if not user:
#             return make_response(jsonify({'message': 'User not found'}), 404)

#         # Delete DefPerson if user_type is "person"
#         if user.user_type and user.user_type.lower() == "person":
#             person = DefPerson.query.filter_by(user_id=user_id).first()
#             if person:
#                 db.session.delete(person)

#         # Delete DefUserCredential if exists
#         user_credential = DefUserCredential.query.filter_by(user_id=user_id).first()
#         if user_credential:
#             db.session.delete(user_credential)

#         # Delete DefAccessProfile(s) if exist
#         access_profiles = DefAccessProfile.query.filter_by(user_id=user_id).all()
#         for profile in access_profiles:
#             db.session.delete(profile)

#         # Delete the DefUser record
#         db.session.delete(user)
#         db.session.commit()

#         return make_response(jsonify({'message': 'User and related records deleted successfully'}), 200)

#     except Exception as e:
#         db.session.rollback()
#         return make_response(jsonify({'message': 'Error deleting user', 'error': str(e)}), 500)


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
        # if '@' in email_or_username:
        # # Cast JSON column to TEXT and use LIKE
        #     user_profile = DefUser.query.filter(cast(DefUser.email_addresses, Text).ilike(f"%{email_or_username}%")).first()
        # else:
        #     user_profile = DefUser.query.filter_by(user_name = email_or_username).first()

        # Use JSONB contains for email lookup
        if '@' in email_or_username:
            user_profile = DefUser.query.filter(
                DefUser.email_addresses.contains([email_or_username])
            ).first()
        else:
            user_profile = DefUser.query.filter_by(user_name=email_or_username).first()



        if user_profile and user_profile.user_id:
            user_credentials = DefUserCredential.query.filter_by(user_id = user_profile.user_id ).first()

            if user_credentials and check_password_hash(user_credentials.password, password):
                access_token = create_access_token(identity = str(user_profile.user_id))
                return make_response(jsonify({"access_token": access_token}), 200)
            else:
                return make_response(jsonify({"message": "Invalid email/username or password"}), 401)
        else:
            return make_response(jsonify({"message": "User not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": str(e)}), 500)

  

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
@jwt_required()
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
@jwt_required()
def Show_ExecutionMethods():
    try:
        methods = DefAsyncExecutionMethods.query.order_by(DefAsyncExecutionMethods.internal_execution_method.desc()).all()
        if not methods:
            return jsonify({"message": "No execution methods found"}), 404
        return jsonify([method.json() for method in methods]), 200
    except Exception as e:
        return jsonify({"message": "Error retrieving execution methods", "error": str(e)}), 500


@flask_app.route('/Show_ExecutionMethods/v1', methods=['GET'])
def Show_ExecutionMethods_v1():
    try:
        methods = DefAsyncExecutionMethods.query.order_by(DefAsyncExecutionMethods.internal_execution_method.desc()).all()
        if not methods:
            return jsonify({"message": "No execution methods found"}), 404
        return jsonify([method.json() for method in methods]), 200
    except Exception as e:
        return jsonify({"message": "Error retrieving execution methods", "error": str(e)}), 500


@flask_app.route('/Show_ExecutionMethods/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def paginated_execution_methods(page, limit):
    try:
        paginated = DefAsyncExecutionMethods.query.order_by(DefAsyncExecutionMethods.creation_date.desc()).paginate(page=page, per_page=limit, error_out=False)

        if not paginated.items:
            return jsonify({"message": "No execution methods found"}), 404

        return jsonify({
            "items": [method.json() for method in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200

    except Exception as e:
        return jsonify({"message": "Error retrieving execution methods", "error": str(e)}), 500



@flask_app.route('/def_async_execution_methods/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_execution_methods(page, limit):
    try:
        search_query = request.args.get('internal_execution_method', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAsyncExecutionMethods.query

        if search_query:
            query = query.filter(
                or_(
                    DefAsyncExecutionMethods.internal_execution_method.ilike(f'%{search_query}%'),
                    DefAsyncExecutionMethods.internal_execution_method.ilike(f'%{search_underscore}%'),
                    DefAsyncExecutionMethods.internal_execution_method.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAsyncExecutionMethods.creation_date.desc()).paginate(
            page=page, per_page=limit, error_out=False
        )

        if not paginated.items:
            return jsonify({"message": "No execution methods found"}), 404

        return jsonify({
            "items": [method.json() for method in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200

    except Exception as e:
        return jsonify({"message": "Error searching execution methods", "error": str(e)}), 500



@flask_app.route('/Show_ExecutionMethod/<string:internal_execution_method>', methods=['GET'])
@jwt_required()
def Show_ExecutionMethod(internal_execution_method):
    try:
        method = DefAsyncExecutionMethods.query.get(internal_execution_method)
        if not method:
            return jsonify({"message": f"Execution method '{internal_execution_method}' not found"}), 404
        return jsonify(method.json()), 200
    except Exception as e:
        return jsonify({"message": "Error retrieving execution method", "error": str(e)}), 500


@flask_app.route('/Update_ExecutionMethod/<string:internal_execution_method>', methods=['PUT'])
@jwt_required()
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


@flask_app.route('/Delete_ExecutionMethod/<string:internal_execution_method>', methods=['DELETE'])
@jwt_required()
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


# Create a task definition
@flask_app.route('/Create_Task', methods=['POST'])
@jwt_required()
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


@flask_app.route('/def_async_tasks', methods=['GET'])
@jwt_required()
def Show_Tasks():
    try:
        tasks = DefAsyncTask.query.order_by(DefAsyncTask.def_task_id.desc()).all()
        return make_response(jsonify([task.json() for task in tasks]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting DEF async Tasks", "error": str(e)}), 500)


@flask_app.route('/def_async_tasks/v1', methods=['GET'])
def Show_Tasks_v1():
    try:
        tasks = DefAsyncTask.query.order_by(DefAsyncTask.def_task_id.desc()).all()
        return make_response(jsonify([task.json() for task in tasks]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting DEF async Tasks", "error": str(e)}), 500)


@flask_app.route('/def_async_tasks/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def Show_Tasks_Paginated(page, limit):
    try:
        tasks = DefAsyncTask.query.order_by(DefAsyncTask.creation_date.desc())
        paginated = tasks.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [model.json() for model in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error getting DEF async Tasks", "error": str(e)}), 500)


@flask_app.route('/def_async_tasks/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def def_async_tasks_show_tasks(page, limit):
    try:
        search_query = request.args.get('user_task_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAsyncTask.query
        if search_query:
            
            query = query.filter(or_(
                DefAsyncTask.user_task_name.ilike(f'%{search_query}%'),
                DefAsyncTask.user_task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTask.user_task_name.ilike(f'%{search_space}%')
            ))
        paginated = query.order_by(DefAsyncTask.def_task_id.desc()).paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [task.json() for task in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error fetching tasks", "error": str(e)}), 500)



@flask_app.route('/Show_Task/<task_name>', methods=['GET'])
@jwt_required()
def Show_Task(task_name):
    try:
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()

        if not task:
            return {"message": f"Task with name '{task_name}' not found"}, 404

        return make_response(jsonify(task.json()), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error getting the task", "error": str(e)}), 500)


@flask_app.route('/Update_Task/<string:task_name>', methods=['PUT'])
@jwt_required()
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
@jwt_required()
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
@jwt_required()
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

        return make_response(jsonify({
            "message": "Parameters added successfully",
            "parameters": [param.json() for param in new_params]
        }), 201)
    except Exception as e:
        return jsonify({"error": "Failed to create task parameters", "details": str(e)}), 500



@flask_app.route('/Show_TaskParams/<string:task_name>', methods=['GET'])
@jwt_required()
def Show_Parameter(task_name):
    try:
        parameters = DefAsyncTaskParam.query.filter_by(task_name=task_name).all()

        if not parameters:
            return make_response(jsonify({"message": f"No parameters found for task '{task_name}'"}), 404)

        return make_response(jsonify([param.json() for param in parameters]), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error getting Task Parameters", "error": str(e)}), 500)



@flask_app.route('/Show_TaskParams/<string:task_name>/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def Show_TaskParams_Paginated(task_name, page, limit):
    try:
        query = DefAsyncTaskParam.query.filter_by(task_name=task_name)
        paginated = query.order_by(DefAsyncTaskParam.def_param_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        if not paginated.items:
            return make_response(jsonify({"message": f"No parameters found for task '{task_name}'"}), 404)

        return make_response(jsonify({
            "items": [param.json() for param in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Task Parameters", "error": str(e)}), 500)



@flask_app.route('/Update_TaskParams/<string:task_name>/<int:def_param_id>', methods=['PUT'])
@jwt_required()
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

        return jsonify({"message": "Task parameter updated successfully", 
                         "task_param": param.json()}), 200

    except Exception as e:
        return jsonify({"error": "Error updating task parameter", "details": str(e)}), 500



@flask_app.route('/Delete_TaskParams/<string:task_name>/<int:def_param_id>', methods=['DELETE'])
@jwt_required()
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
@jwt_required()
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

        # Prevent scheduling if the task is cancelled
        if getattr(task, 'cancelled_yn', 'N') == 'Y':
            return jsonify({'error': f"Task '{task_name}' is cancelled and cannot be scheduled."}), 400

        user_task_name = task.user_task_name
        executor = task.executor
        script_name = task.script_name

        schedule_name = str(uuid.uuid4())
        # redbeat_schedule_name = f"{user_schedule_name}_{schedule_name}"
        redbeat_schedule_name = None
        if schedule_type != "IMMEDIATE":
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

        # elif schedule_type == "MONTHLY_LAST_DAY":

        #     try:
        #         today = datetime.today()
        #         start_year = today.year
        #         start_month = today.month

        #         # Calculate how many months left in the year including current month
        #         months_left = 12 - start_month + 1  # +1 to include the current month itself

        #         for i in range(months_left):
        #             # Calculate the target year and month
        #             year = start_year  # same year, no spanning next year
        #             month = start_month + i

        #             # Find the first day of the next month
        #             if month == 12:
        #                 next_month = datetime(year + 1, 1, 1)
        #             else:
        #                 next_month = datetime(year, month + 1, 1)

        #             # Calculate the last day of the current month
        #             last_day_dt = next_month - timedelta(days=1)
        #             last_day = last_day_dt.day

        #             # Create a cron schedule for the last day of this month at midnight
        #             cron_schedule = crontab(
        #                 minute=0,
        #                 hour=0,
        #                 day_of_month=last_day,
        #                 month_of_year=month
        #             )

        #             redbeat_schedule_name = f"{user_schedule_name}_{uuid.uuid4()}"

        #             args_with_schedule = [
        #                 script_name,
        #                 user_task_name,
        #                 task_name,
        #                 user_schedule_name,
        #                 redbeat_schedule_name,
        #                 schedule_type,
        #                 schedule_data
        #             ]

        #             # Create Redis schedule entry via RedBeat
        #             create_redbeat_schedule(
        #                 schedule_name=redbeat_schedule_name,
        #                 executor=executor,
        #                 cron_schedule=cron_schedule,
        #                 args=args_with_schedule,
        #                 kwargs=kwargs,
        #                 celery_app=celery
        #             )

        #             # Create database record for the schedule
        #             new_schedule = DefAsyncTaskScheduleNew(
        #                 user_schedule_name=user_schedule_name,
        #                 redbeat_schedule_name=redbeat_schedule_name,
        #                 task_name=task_name,
        #                 args=args_with_schedule,
        #                 kwargs=kwargs,
        #                 parameters=kwargs,
        #                 schedule_type=schedule_type,
        #                 schedule={"scheduled_for": f"{year}-{month:02}-{last_day} 00:00"},
        #                 cancelled_yn='N',
        #                 created_by=101
        #             )

        #             db.session.add(new_schedule)

        #         db.session.commit()

        #         return jsonify({
        #             "message": f"Monthly last-day tasks scheduled for the remaining {months_left} months of {start_year}"
        #         }), 201

        #     except Exception as e:
        #         db.session.rollback()
        #         return jsonify({
        #             "error": "Failed to schedule monthly last-day tasks",
        #             "details": str(e)
        #         }), 500


        
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

        # if schedule_type != "IMMEDIATE":
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
            # ready_for_redbeat="N",
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
@jwt_required()
def Show_TaskSchedules():
    try:
    #     schedules = DefAsyncTaskSchedulesV.query \
    # .filter(DefAsyncTaskSchedulesV.ready_for_redbeat != 'Y') \
    # .order_by(desc(DefAsyncTaskSchedulesV.def_task_sche_id)) \
    # .all()
        schedules = DefAsyncTaskSchedulesV.query.order_by(DefAsyncTaskSchedulesV.def_task_sche_id.desc()).all()
        # Return the schedules as a JSON response
        return jsonify([schedule.json() for schedule in schedules])

    except Exception as e:
        # Handle any errors and return them as a JSON response
        return jsonify({"error": str(e)}), 500



@flask_app.route('/def_async_task_schedules/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def paginated_task_schedules(page, limit):
    try:
        paginated = DefAsyncTaskSchedulesV.query.order_by(
            DefAsyncTaskSchedulesV.def_task_sche_id.desc()
        ).paginate(page=page, per_page=limit, error_out=False)

        return jsonify({
            "items": [schedule.json() for schedule in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200
    except Exception as e:
        return jsonify({"message": "Error fetching task schedules", "error": str(e)}), 500


@flask_app.route('/def_async_task_schedules/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_task_schedules(page, limit):
    try:
        search_query = request.args.get('task_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAsyncTaskSchedulesV.query

        if search_query:
            query = query.filter(
                or_(
                    DefAsyncTaskSchedulesV.task_name.ilike(f'%{search_query}%'),
                    DefAsyncTaskSchedulesV.task_name.ilike(f'%{search_underscore}%'),
                    DefAsyncTaskSchedulesV.task_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAsyncTaskSchedulesV.def_task_sche_id.desc()).paginate(
            page=page, per_page=limit, error_out=False
        )

        return jsonify({
            "items": [schedule.json() for schedule in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200
    except Exception as e:
        return jsonify({"message": "Error searching task schedules", "error": str(e)}), 500





@flask_app.route('/Show_TaskSchedule/<string:task_name>', methods=['GET'])
@jwt_required()
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
@jwt_required()
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

        # if schedule.ready_for_redbeat != 'N':
        #     return jsonify({
        #         "message": f"Task Periodic Schedule for {redbeat_schedule_name} is not marked as 'N'. Update is not allowed."
        #     }), 400

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
@jwt_required()
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
        # if schedule.ready_for_redbeat != 'N':
        #     return make_response(jsonify({"message": f"Cancellation not allowed. Task periodic schedule for {redbeat_schedule_name} is already processed in Redis"}), 400)

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


@flask_app.route('/Reschedule_Task/<string:task_name>', methods=['PUT'])
@jwt_required()
def Reschedule_TaskSchedule(task_name):
    try:
        data = request.get_json()
        redbeat_schedule_name = data.get('redbeat_schedule_name')
        if not redbeat_schedule_name:
            return make_response(jsonify({'error': 'redbeat_schedule_name is required'}), 400)

        # Find the cancelled schedule in DB
        schedule = DefAsyncTaskScheduleNew.query.filter_by(
            task_name=task_name,
            redbeat_schedule_name=redbeat_schedule_name,
            cancelled_yn='Y'
        ).first()

        if not schedule:
            return make_response(jsonify({'error': 'Cancelled schedule not found'}), 404)

        # Determine cron or periodic schedule
        cron_schedule = None
        schedule_minutes = None
        schedule_data = schedule.schedule
        schedule_type = schedule.schedule_type

        if schedule_type == "WEEKLY_SPECIFIC_DAYS":
            values = schedule_data.get('VALUES', [])
            day_map = {
                "SUN": 0, "MON": 1, "TUE": 2, "WED": 3,
                "THU": 4, "FRI": 5, "SAT": 6
            }
            days_of_week = ",".join(str(day_map[day.upper()]) for day in values if day.upper() in day_map)
            cron_schedule = crontab(minute=0, hour=0, day_of_week=days_of_week)

        elif schedule_type == "MONTHLY_SPECIFIC_DATES":
            values = schedule_data.get('VALUES', [])
            dates_of_month = ",".join(values)
            cron_schedule = crontab(minute=0, hour=0, day_of_month=dates_of_month)

        elif schedule_type == "ONCE":
            one_time_date = schedule_data.get('VALUES')
            dt = datetime.strptime(one_time_date, "%Y-%m-%d %H:%M")
            cron_schedule = crontab(minute=dt.minute, hour=dt.hour, day_of_month=dt.day, month_of_year=dt.month)

        elif schedule_type == "PERIODIC":
            frequency_type = schedule_data.get('FREQUENCY_TYPE', '').upper()
            frequency = schedule_data.get('FREQUENCY', 1)
            if frequency_type == 'MONTHS':
                schedule_minutes = frequency * 30 * 24 * 60
            elif frequency_type == 'WEEKS':
                schedule_minutes = frequency * 7 * 24 * 60
            elif frequency_type == 'DAYS':
                schedule_minutes = frequency * 24 * 60
            elif frequency_type == 'HOURS':
                schedule_minutes = frequency * 60
            elif frequency_type == 'MINUTES':
                schedule_minutes = frequency
            else:
                return make_response(jsonify({'error': f'Invalid frequency type: {frequency_type}'}), 400)

        else:
            return make_response(jsonify({'error': f'Cannot reschedule type: {schedule_type}'}), 400)
        
        executor = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if not executor:
            return make_response(jsonify({'error': f'Executor not found for task {task_name}'}),404)
        # Restore schedule in Redis
        try:
            create_redbeat_schedule(
                schedule_name=redbeat_schedule_name,
                executor=executor.executor,     
                schedule_minutes=schedule_minutes,
                cron_schedule=cron_schedule,
                args=schedule.args,
                kwargs=schedule.kwargs,
                celery_app=celery
            )
            print(executor)
            
        except Exception as e:
            return make_response(jsonify({'error': 'Failed to recreate RedBeat schedule', 'details': str(e)}), 500)

        # Update DB
        schedule.cancelled_yn = 'N'
        schedule.last_updated_by = get_jwt_identity()
        schedule.last_update_date = datetime.utcnow()
        db.session.commit()

        return make_response(jsonify({'message': f"Schedule '{redbeat_schedule_name}' has been rescheduled."}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'error': 'Failed to reschedule task', 'details': str(e)}), 500)



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


@flask_app.route('/view_requests_v1', methods=['GET'])
@jwt_required()
def get_all_tasks():
    try:
        fourteen_days = datetime.utcnow() - timedelta(days=4)
        tasks = DefAsyncTaskRequest.query.filter(DefAsyncTaskRequest.creation_date >= fourteen_days).order_by(DefAsyncTaskRequest.creation_date.desc())
        #tasks = DefAsyncTaskRequest.query.limit(100000).all()
        if not tasks:
            return jsonify({"message": "No tasks found"}), 404
        return jsonify([task.json() for task in tasks]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# @flask_app.route('/view_requests/<int:page>/<int:page_limit>', methods=['GET'])
# # @jwt_required()
# def view_requests(page, page_limit):
#     try:
#         fourteen_days = datetime.utcnow() - timedelta(days=14)
#         # Filter by date
#         query = DefAsyncTaskRequest.query.filter(
#             DefAsyncTaskRequest.creation_date >= fourteen_days
#         )
        
#         # Total number of matching tasks
#         total = query.count()
#         total_pages = (total + page_limit - 1) // page_limit

#         # Paginate using offset and limit
#         requests = query.order_by(DefAsyncTaskRequest.creation_date.desc())\
#                      .offset((page - 1) * page_limit).limit(page_limit).all()

#         if not requests:
#             return jsonify({"message": "No tasks found"}), 404

#         return make_response(jsonify({
#             "items": [req.json() for req in requests],
#             "total": requests.total,
#             "pages": requests.pages,
#             "page": requests.page
#         }), 200)

#     except Exception as e:
#         return jsonify({"error": str(e)}), 500





#def_async_task_requests
@flask_app.route('/view_requests/<int:page>/<int:page_limit>', methods=['GET'])
@jwt_required()
def view_requests(page, page_limit):
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=14)

        query = DefAsyncTaskRequest.query.filter(
            DefAsyncTaskRequest.creation_date >= cutoff_date
        ).order_by(DefAsyncTaskRequest.creation_date.desc())

        paginated = query.paginate(page=page, per_page=page_limit, error_out=False)

        if not paginated.items:
            return jsonify({"message": "No tasks found"}), 404

        return jsonify({
            "items": [task.json() for task in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route('/view_requests/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def def_async_task_requests_view_requests(page, limit):
    try:
        search_query = request.args.get('task_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        day_limit = datetime.utcnow() - timedelta(days=30)
        query = DefAsyncTaskRequest.query.filter(DefAsyncTaskRequest.creation_date >= day_limit)

        if search_query:
            query = query.filter(or_(
                DefAsyncTaskRequest.task_name.ilike(f'%{search_query}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_space}%')
            ))

        paginated = query.order_by(DefAsyncTaskRequest.creation_date.desc()) \
                         .paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [req.json() for req in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error fetching view requests", "error": str(e)}), 500)



#def_access_models
@flask_app.route('/def_access_models', methods=['POST'])
def create_def_access_models():
    try:
        datasource_name = request.json.get('datasource_name', None)
        # Only validate foreign key if datasource_name is provided and not null/empty
        if datasource_name:
            datasource = DefDataSource.query.filter_by(datasource_name=datasource_name).first()
            if not datasource:
                return make_response(jsonify({"message": f"Datasource '{datasource_name}' does not exist"}), 400)

        new_def_access_model = DefAccessModel(
            model_name = request.json.get('model_name'),
            description = request.json.get('description'),
            type = request.json.get('type'),
            run_status = request.json.get('run_status'),
            state = request.json.get('state'),
            last_run_date = datetime.utcnow(),
            created_by = request.json.get('created_by'),
            last_updated_by = request.json.get('last_updated_by'),
            last_updated_date = datetime.utcnow(),
            revision = 0,
            revision_date = datetime.utcnow(),
            datasource_name = datasource_name  # FK assignment
        )
        db.session.add(new_def_access_model)
        db.session.commit()
        return make_response(jsonify({"message": "DefAccessModel created successfully!"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    
@flask_app.route('/def_access_models', methods=['GET'])
def get_def_access_models():
    try:
        models = DefAccessModel.query.order_by(DefAccessModel.def_access_model_id.desc()).all()
        return make_response(jsonify([model.json() for model in models]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving access models", "error": str(e)}), 500)

@flask_app.route('/def_access_models/<int:page>/<int:limit>', methods=['GET'])
def get_paginated_def_access_models(page, limit):
    try:
        query = DefAccessModel.query.order_by(DefAccessModel.def_access_model_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [model.json() for model in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving access models", "error": str(e)}), 500)


@flask_app.route('/def_access_models/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_access_models(page, limit):
    try:
        search_query = request.args.get('model_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAccessModel.query

        if search_query:
            query = query.filter(
                or_(
                    DefAccessModel.model_name.ilike(f'%{search_query}%'),
                    DefAccessModel.model_name.ilike(f'%{search_underscore}%'),
                    DefAccessModel.model_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAccessModel.def_access_model_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [model.json() for model in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching access models", "error": str(e)}), 500)


@flask_app.route('/def_access_models/<int:model_id>', methods=['GET'])
def get_def_access_model(model_id):
    try:
        model = DefAccessModel.query.filter_by(def_access_model_id=model_id).first()
        return make_response(jsonify(model.json()), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving access models", "error": str(e)}), 500)


@flask_app.route('/def_access_models/<int:model_id>', methods=['PUT'])
def update_def_access_model(model_id):
    try:
        model = DefAccessModel.query.filter_by(def_access_model_id=model_id).first()
        if model:
            data = request.get_json()
            # Handle datasource_name FK update with case-insensitive and space-insensitive matching
            if 'datasource_name' in data:
                # Normalize input: strip, lower, remove underscores and spaces for matching
                input_ds = data['datasource_name'].strip().lower().replace('_', '').replace(' ', '')
                datasource = DefDataSource.query.filter(
                    func.replace(func.replace(func.lower(DefDataSource.datasource_name), '_', ''), ' ', '') == input_ds
                ).first()
                if not datasource:
                    return make_response(jsonify({"message": f"Datasource '{data['datasource_name']}' does not exist"}), 404)
                model.datasource_name = datasource.datasource_name  # Use the canonical name from DB

            model.model_name        = data.get('model_name', model.model_name)
            model.description       = data.get('description', model.description)
            model.type              = data.get('type', model.type)
            model.run_status        = data.get('run_status', model.run_status)
            model.state             = data.get('state', model.state)
            model.last_run_date     = datetime.utcnow()
            model.last_updated_by   = data.get('last_updated_by', model.last_updated_by)
            model.last_updated_date = datetime.utcnow()
            model.revision          = model.revision + 1
            model.revision_date     = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'DefAccessModel updated successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'DefAccessModel not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating DefAccessModel', 'error': str(e)}), 500)

@flask_app.route('/def_access_models/<int:model_id>', methods=['DELETE'])
def delete_def_access_model(model_id):
    try:
        model = DefAccessModel.query.filter_by(def_access_model_id=model_id).first()
        if model:
            db.session.delete(model)
            db.session.commit()
            return make_response(jsonify({'message': 'DefAccessModel deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'DefAccessModel not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting DefAccessModel', 'error': str(e)}), 500)




#def_access_model_logics
@flask_app.route('/def_access_model_logics', methods=['POST'])
def create_def_access_model_logic():
    try:
        def_access_model_logic_id = request.json.get('def_access_model_logic_id')
        def_access_model_id = request.json.get('def_access_model_id')
        filter_text = request.json.get('filter')
        object_text = request.json.get('object')
        attribute = request.json.get('attribute')
        condition = request.json.get('condition')
        value = request.json.get('value')

        if not def_access_model_id:
            return make_response(jsonify({'message': 'def_access_model_id is required'}), 400)
        
        if DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first():
            return make_response(jsonify({'message': f'def_access_model_logic_id {def_access_model_logic_id} already exists'}), 409)

        # Check if def_access_model_id exists in DefAccessModel table
        model_id_exists = db.session.query(
            db.exists().where(DefAccessModel.def_access_model_id == def_access_model_id)
        ).scalar()
        if not model_id_exists:
            return make_response(jsonify({'message': f'def_access_model_id {def_access_model_id} does not exist'}), 400)

        new_logic = DefAccessModelLogic(
            def_access_model_logic_id=def_access_model_logic_id,
            def_access_model_id=def_access_model_id,
            filter=filter_text,
            object=object_text,
            attribute=attribute,
            condition=condition,
            value=value
        )
        db.session.add(new_logic)
        db.session.commit()
        return make_response(jsonify({'message': 'DefAccessModelLogic created successfully!'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': f'Error: {str(e)}'}), 500)

@flask_app.route('/def_access_model_logics/upsert', methods=['POST'])
def upsert_def_access_model_logics():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []

        for data in data_list:
            def_access_model_logic_id = data.get('def_access_model_logic_id')
            model_id = data.get('def_access_model_id')
            filter_text = data.get('filter')
            object_text = data.get('object')
            attribute = data.get('attribute')
            condition = data.get('condition')
            value = data.get('value')

            existing_logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first()

            if existing_logic:
                # if not logic:
                #     response.append({
                #         'def_access_model_logic_id': logic_id,
                #         'status': 'error',
                #         'message': f'DefAccessModelLogic with id {logic_id} not found'
                #     })
                #     continue

                # Prevent changing foreign key
                if model_id and model_id != existing_logic.def_access_model_id:
                    response.append({
                        'def_access_model_logic_id': def_access_model_logic_id,
                        'status': 'error',
                        'message': 'Updating def_access_model_id is not allowed'
                    })
                    continue

               
                existing_logic.filter = filter_text
                existing_logic.object = object_text
                existing_logic.attribute = attribute
                existing_logic.condition = condition
                existing_logic.value = value
                db.session.add(existing_logic)

                response.append({
                    'def_access_model_logic_id': existing_logic.def_access_model_logic_id,
                    'status': 'updated',
                    'message': 'Logic updated successfully'
                })

            else:
                if not model_id:
                    response.append({
                        'status': 'error',
                        'message': 'def_access_model_id is required for new records'
                    })
                    continue

                # Validate foreign key existence (optional; depends on enforcement at DB)
                model_exists = db.session.query(
                    db.exists().where(DefAccessModel.def_access_model_id == model_id)
                ).scalar()

                if not model_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_access_model_id {model_id} does not exist'
                    })
                    continue

                new_logic = DefAccessModelLogic(
                    def_access_model_logic_id = def_access_model_logic_id,
                    def_access_model_id=model_id,
                    filter=filter_text,
                    object=object_text,
                    attribute=attribute,
                    condition=condition,
                    value=value
                )
                db.session.add(new_logic)
                db.session.flush()

                response.append({
                    'def_access_model_logic_id': new_logic.def_access_model_logic_id,
                    'status': 'created',
                    'message': 'Logic created successfully'
                })

        db.session.commit()
        return make_response(jsonify(response), 200)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error during upsert'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error during upsert',
            'error': str(e)
        }), 500)



@flask_app.route('/def_access_model_logics', methods=['GET'])
def get_def_access_model_logics():
    try:
        logics = DefAccessModelLogic.query.order_by(DefAccessModelLogic.def_access_model_logic_id.desc()).all()
        return make_response(jsonify([logic.json() for logic in logics]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error retrieving logics', 'error': str(e)}), 500)


@flask_app.route('/def_access_model_logics/<int:logic_id>', methods=['GET'])
def get_def_access_model_logic(logic_id):
    try:
        logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=logic_id).first()
        if logic:
            return make_response(jsonify(logic.json()), 200)
        else:
            return make_response(jsonify({'message': 'Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error retrieving logic', 'error': str(e)}), 500)


@flask_app.route('/def_access_model_logics/<int:logic_id>', methods=['PUT'])
def update_def_access_model_logic(logic_id):
    try:
        logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=logic_id).first()
        if logic:
            # logic.def_access_model_id = request.json.get('def_access_model_id', logic.def_access_model_id)
            logic.filter = request.json.get('filter', logic.filter)
            logic.object = request.json.get('object', logic.object)
            logic.attribute = request.json.get('attribute', logic.attribute)
            logic.condition = request.json.get('condition', logic.condition)
            logic.value = request.json.get('value', logic.value)

            db.session.commit()
            return make_response(jsonify({'message': 'Logic updated successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating logic', 'error': str(e)}), 500)


@flask_app.route('/def_access_model_logics/<int:logic_id>', methods=['DELETE'])
def delete_def_access_model_logic(logic_id):
    try:
        logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=logic_id).first()
        if logic:
            db.session.delete(logic)
            db.session.commit()
            return make_response(jsonify({'message': 'Logic deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting logic', 'error': str(e)}), 500)





#def_access_model_logic_attributes
@flask_app.route('/def_access_model_logic_attributes', methods=['POST'])
def create_def_access_model_logic_attribute():
    try:
        id = request.json.get('id')
        def_access_model_logic_id = request.json.get('def_access_model_logic_id')
        widget_position = request.json.get('widget_position')
        widget_state = request.json.get('widget_state')

        if not def_access_model_logic_id:
            return make_response(jsonify({'message': 'def_access_model_logic_id is required'}), 400)
        if DefAccessModelLogicAttribute.query.filter_by(id=id).first():
            return make_response(jsonify({'message': f'id {id} already exists'}), 409)
        # Check if def_access_model_logic_id exists in DefAccessModelLogic table
        logic_id_exists = db.session.query(
            db.exists().where(DefAccessModelLogic.def_access_model_logic_id == def_access_model_logic_id)
        ).scalar()
        if not logic_id_exists:
            return make_response(jsonify({'message': f'def_access_model_logic_id {def_access_model_logic_id} does not exist'}), 400)
        
        
        new_attribute = DefAccessModelLogicAttribute(
            id = id,
            def_access_model_logic_id=def_access_model_logic_id,
            widget_position=widget_position,
            widget_state=widget_state
        )
        db.session.add(new_attribute)
        db.session.commit()
        return make_response(jsonify({"message": "DefAccessModelLogicAttribute created successfully!"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)


@flask_app.route('/def_access_model_logic_attributes', methods=['GET'])
def get_def_access_model_logic_attributes():
    try:
        attributes = DefAccessModelLogicAttribute.query.order_by(DefAccessModelLogicAttribute.id.desc()).all()
        return make_response(jsonify([attr.json() for attr in attributes]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving attributes", "error": str(e)}), 500)



@flask_app.route('/def_access_model_logic_attributes/upsert', methods=['POST'])
def upsert_def_access_model_logic_attributes():
    try:
        data_list = request.get_json()

        # Enforce list-only payload
        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []

        for data in data_list:
            id = data.get('id')
            def_access_model_logic_id = data.get('def_access_model_logic_id')
            widget_position = data.get('widget_position')
            widget_state = data.get('widget_state')

            existing_attribute = DefAccessModelLogicAttribute.query.filter_by(id=id).first()
            if existing_attribute:
                # if not attribute:
                #     response.append({
                #         'id': attribute_id,
                #         'status': 'error',
                #         'message': f'Attribute with id {attribute_id} not found'
                #     })
                #     continue

                # Disallow updating def_access_model_logic_id
                if def_access_model_logic_id and def_access_model_logic_id != existing_attribute.def_access_model_logic_id:
                    response.append({
                        'id': id,
                        'status': 'error',
                        'message': 'Updating def_access_model_logic_id is not allowed'
                    })
                    continue

                existing_attribute.widget_position = widget_position
                existing_attribute.widget_state = widget_state
                db.session.add(existing_attribute)

                response.append({
                    'id': existing_attribute.id,
                    'status': 'updated',
                    'message': 'Attribute updated successfully'
                })

            else:
                # Take the maximum data of foreign-key from foreign table
                # def_access_model_logic_id = db.session.query(
                #     func.max(DefAccessModelLogic.def_access_model_logic_id)
                # ).scalar()

                # if def_access_model_logic_id is None:
                #     response.append({
                #         'status': 'error',
                #         'message': 'No DefAccessModelLogic entries exist to assign logic ID'
                #     })
                #     continue

                # Validate def_access_model_logic_id exists
                logic_exists = db.session.query(
                    db.exists().where(DefAccessModelLogic.def_access_model_logic_id == def_access_model_logic_id)
                    ).scalar()

                if not logic_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_access_model_logic_id {def_access_model_logic_id} does not exist'
                    })
                    continue

                new_attribute = DefAccessModelLogicAttribute(
                    id = id,
                    def_access_model_logic_id=def_access_model_logic_id,
                    widget_position=widget_position,
                    widget_state=widget_state
                )
                db.session.add(new_attribute)
                db.session.flush()

                response.append({
                    'id': new_attribute.id,
                    'status': 'created',
                    'message': 'Attribute created successfully'
                })

        db.session.commit()
        return make_response(jsonify(response), 200)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error during upsert'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error during upsert',
            'error': str(e)
        }), 500)



@flask_app.route('/def_access_model_logic_attributes/<int:attr_id>', methods=['GET'])
def get_def_access_model_logic_attribute(attr_id):
    try:
        attribute = DefAccessModelLogicAttribute.query.filter_by(id=attr_id).first()
        if attribute:
            return make_response(jsonify(attribute.json()), 200)
        else:
            return make_response(jsonify({'message': 'Attribute not found'}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving attribute", "error": str(e)}), 500)


@flask_app.route('/def_access_model_logic_attributes/<int:attr_id>', methods=['PUT'])
def update_def_access_model_logic_attribute(attr_id):
    try:
        attribute = DefAccessModelLogicAttribute.query.filter_by(id=attr_id).first()
        if attribute:
            # attribute.def_access_model_logic_id = request.json.get('def_access_model_logic_id', attribute.def_access_model_logic_id)
            attribute.widget_position = request.json.get('widget_position', attribute.widget_position)
            attribute.widget_state = request.json.get('widget_state', attribute.widget_state)

            db.session.commit()
            return make_response(jsonify({'message': 'Attribute updated successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Attribute not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating attribute', 'error': str(e)}), 500)


@flask_app.route('/def_access_model_logic_attributes/<int:attr_id>', methods=['DELETE'])
def delete_def_access_model_logic_attribute(attr_id):
    try:
        attribute = DefAccessModelLogicAttribute.query.filter_by(id=attr_id).first()
        if attribute:
            db.session.delete(attribute)
            db.session.commit()
            return make_response(jsonify({'message': 'Attribute deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Attribute not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting attribute', 'error': str(e)}), 500)





# def_global_conditions
@flask_app.route('/def_global_conditions', methods=['POST'])
def create_def_global_condition():
    try:
        name        = request.json.get('name')
        datasource  = request.json.get('datasource')
        description = request.json.get('description')
        status      = request.json.get('status')

        new_condition = DefGlobalCondition(
            name        = name,
            datasource  = datasource,
            description = description,
            status      = status
        )

        db.session.add(new_condition)
        db.session.commit()

        return make_response(jsonify({"message": "DefGlobalCondition created successfully!"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)

@flask_app.route('/def_global_conditions', methods=['GET'])
def get_def_global_conditions():
    try:
        conditions = DefGlobalCondition.query.order_by(DefGlobalCondition.def_global_condition_id.desc()).all()
        return make_response(jsonify([condition.json() for condition in conditions]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving DefGlobalConditions", "error": str(e)}), 500)


@flask_app.route('/def_global_conditions/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_global_conditions(page, limit):
    try:
        search_query = request.args.get('name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefGlobalCondition.query

        if search_query:
            query = query.filter(
                or_(
                    DefGlobalCondition.name.ilike(f'%{search_query}%'),
                    DefGlobalCondition.name.ilike(f'%{search_underscore}%'),
                    DefGlobalCondition.name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefGlobalCondition.def_global_condition_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({
            "message": "Error searching DefGlobalConditions",
            "error": str(e)
        }), 500)


@flask_app.route('/def_global_conditions/<int:def_global_condition_id>', methods=['GET'])
def get_def_global_condition(def_global_condition_id):
    try:
        condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
        if condition:
            return make_response(jsonify(condition.json()), 200)
        return make_response(jsonify({"message": "DefGlobalCondition not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving DefGlobalCondition", "error": str(e)}), 500)


@flask_app.route('/def_global_conditions/<int:page>/<int:limit>', methods=['GET'])
def get_paginated_def_global_conditions(page, limit):
    try:
        query = DefGlobalCondition.query.order_by(DefGlobalCondition.def_global_condition_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving DefGlobalConditions",
            "error": str(e)
        }), 500)


@flask_app.route('/def_global_conditions/<int:def_global_condition_id>', methods=['PUT'])
def update_def_global_condition(def_global_condition_id):
    try:
        condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
        if condition:
            condition.name        = request.json.get('name', condition.name)
            condition.datasource  = request.json.get('datasource', condition.datasource)
            condition.description = request.json.get('description', condition.description)
            condition.status      = request.json.get('status', condition.status)

            db.session.commit()
            return make_response(jsonify({'message': 'DefGlobalCondition updated successfully'}), 200)
        return make_response(jsonify({'message': 'DefGlobalCondition not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating DefGlobalCondition', 'error': str(e)}), 500)

@flask_app.route('/def_global_conditions/<int:def_global_condition_id>', methods=['DELETE'])
def delete_def_global_condition(def_global_condition_id):
    try:
        condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
        if condition:
            db.session.delete(condition)
            db.session.commit()
            return make_response(jsonify({'message': 'DefGlobalCondition deleted successfully'}), 200)
        return make_response(jsonify({'message': 'DefGlobalCondition not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting DefGlobalCondition', 'error': str(e)}), 500)




# def_global_condition_logics
@flask_app.route('/def_global_condition_logics', methods=['POST'])
def create_def_global_condition_logic():
    try:
        def_global_condition_logic_id = request.json.get('def_global_condition_logic_id')

        if def_global_condition_logic_id is None:
            return make_response(jsonify({"message": "Missing 'def_global_condition_logic_id'"}), 400)

        # Check if ID already exists
        existing = DefGlobalConditionLogic.query.get(def_global_condition_logic_id)
        if existing:
            return make_response(jsonify({
                "message": f"DefGlobalConditionLogic ID {def_global_condition_logic_id} already exists."
            }), 409)
        
        def_global_condition_id = request.json.get('def_global_condition_id')
        object = request.json.get('object')
        attribute = request.json.get('attribute')
        condition = request.json.get('condition')
        value = request.json.get('value')

    
        new_logic = DefGlobalConditionLogic(
            def_global_condition_logic_id = def_global_condition_logic_id,
            def_global_condition_id = def_global_condition_id,
            object = object,
            attribute = attribute,
            condition = condition,
            value = value
        )
        db.session.add(new_logic)
        db.session.commit()
        return make_response(jsonify({'def_global_condition_logic_id' : new_logic.def_global_condition_logic_id,
                                      'message': 'DefGlobalConditionLogic created successfully!'}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)

@flask_app.route('/def_global_condition_logics/upsert', methods=['POST'])
def upsert_def_global_condition_logics():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []

        for data in data_list:
            def_global_condition_logic_id = data.get('def_global_condition_logic_id')
            def_global_condition_id = data.get('def_global_condition_id')
            object_text = data.get('object')
            attribute = data.get('attribute')
            condition = data.get('condition')
            value = data.get('value')

            existing_logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()

            if existing_logic:
                # Prevent changing foreign key
                if def_global_condition_id and def_global_condition_id != existing_logic.def_global_condition_id:
                    response.append({
                        'def_global_condition_logic_id': def_global_condition_logic_id,
                        'status': 'error',
                        'message': 'Updating def_global_condition_id is not allowed'
                    })
                    continue

                existing_logic.object = object_text
                existing_logic.attribute = attribute
                existing_logic.condition = condition
                existing_logic.value = value
                db.session.add(existing_logic)

                response.append({
                    'def_global_condition_logic_id': existing_logic.def_global_condition_logic_id,
                    'status': 'updated',
                    'message': 'Logic updated successfully'
                })

            else:
                if not def_global_condition_id:
                    response.append({
                        'status': 'error',
                        'message': 'def_global_condition_id is required for new records'
                    })
                    continue

                # Validate foreign key existence (optional; depends on enforcement at DB)
                condition_exists = db.session.query(
                    db.exists().where(DefGlobalCondition.def_global_condition_id == def_global_condition_id)
                ).scalar()

                if not condition_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_global_condition_id {def_global_condition_id} does not exist'
                    })
                    continue

                new_logic = DefGlobalConditionLogic(
                    def_global_condition_logic_id=def_global_condition_logic_id,
                    def_global_condition_id=def_global_condition_id,
                    object=object_text,
                    attribute=attribute,
                    condition=condition,
                    value=value
                )
                db.session.add(new_logic)
                db.session.flush()

                response.append({
                    'def_global_condition_logic_id': new_logic.def_global_condition_logic_id,
                    'status': 'created',
                    'message': 'Logic created successfully'
                })

        db.session.commit()
        return make_response(jsonify(response), 200)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error during upsert'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error during upsert',
            'error': str(e)
        }), 500)


@flask_app.route('/def_global_condition_logics', methods=['GET'])
def get_def_global_condition_logics():
    try:
        logics = DefGlobalConditionLogic.query.order_by(DefGlobalConditionLogic.def_global_condition_logic_id.desc()).all()
        return make_response(jsonify([logic.json() for logic in logics]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving DefGlobalConditionLogics", "error": str(e)}), 500)



@flask_app.route('/def_global_condition_logics/<int:def_global_condition_logic_id>', methods=['GET'])
def get_def_global_condition_logic(def_global_condition_logic_id):
    try:
        logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()
        if logic:
            return make_response(jsonify(logic.json()), 200)
        return make_response(jsonify({"message": "DefGlobalConditionLogic not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving DefGlobalConditionLogic", "error": str(e)}), 500)


@flask_app.route('/def_global_condition_logics/<int:def_global_condition_logic_id>', methods=['PUT'])
def update_def_global_condition_logic(def_global_condition_logic_id):
    try:
        logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()
        if logic:
            logic.def_global_condition_id = request.json.get('def_global_condition_id', logic.def_global_condition_id)
            logic.object                  = request.json.get('object', logic.object)
            logic.attribute               = request.json.get('attribute', logic.attribute)
            logic.condition               = request.json.get('condition', logic.condition)
            logic.value                   = request.json.get('value', logic.value)

            db.session.commit()
            return make_response(jsonify({'message': 'DefGlobalConditionLogic updated successfully'}), 200)
        return make_response(jsonify({'message': 'DefGlobalConditionLogic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating DefGlobalConditionLogic', 'error': str(e)}), 500)


@flask_app.route('/def_global_condition_logics/<int:def_global_condition_logic_id>', methods=['DELETE'])
def delete_def_global_condition_logic(def_global_condition_logic_id):
    try:
        logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()
        if logic:
            db.session.delete(logic)
            db.session.commit()
            return make_response(jsonify({'message': 'DefGlobalConditionLogic deleted successfully'}), 200)
        return make_response(jsonify({'message': 'DefGlobalConditionLogic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting DefGlobalConditionLogic', 'error': str(e)}), 500)





# def_global_condition_logics_attributes
@flask_app.route('/def_global_condition_logic_attributes', methods=['POST'])
def create_def_global_condition_logic_attribute():
    try:
        id = request.json.get('id')
        def_global_condition_logic_id = request.json.get('def_global_condition_logic_id')
        widget_position = request.json.get('widget_position')
        widget_state = request.json.get('widget_state')

        if not all([id, def_global_condition_logic_id]):
            return make_response(jsonify({'message': 'Both id and def_global_condition_logic_id are required'}), 400)

        # Check if the ID already exists
        existing = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()
        if existing:
            return make_response(jsonify({'message': f'Attribute with id {id} already exists'}), 409)

        # Check if foreign key exists
        logic_exists = db.session.query(
            db.exists().where(DefGlobalConditionLogic.def_global_condition_logic_id == def_global_condition_logic_id)
        ).scalar()

        if not logic_exists:
            return make_response(jsonify({
                'message': f'def_global_condition_logic_id {def_global_condition_logic_id} does not exist'
            }), 404)

        # Create new record
        new_attr = DefGlobalConditionLogicAttribute(
            id=id,
            def_global_condition_logic_id=def_global_condition_logic_id,
            widget_position=widget_position,
            widget_state=widget_state
        )

        db.session.add(new_attr)
        db.session.commit()

        return make_response(jsonify({
            'id': new_attr.id,
            'message': 'Attribute created successfully'
        }), 201)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error (possibly duplicate key)'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error creating attribute', 'error': str(e)}), 500)



@flask_app.route('/def_global_condition_logic_attributes', methods=['GET'])
def get_all_def_global_condition_logic_attributes():
    try:
        attributes = DefGlobalConditionLogicAttribute.query.order_by(DefGlobalConditionLogicAttribute.id.desc()).all()
        return make_response(jsonify([attribute.json() for attribute in attributes]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving condition logic attributes", "error": str(e)}), 500)


@flask_app.route('/def_global_condition_logic_attributes/<int:id>', methods=['GET'])
def get_def_global_condition_logic_attribute(id):
    try:
        attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()
        if attribute:
            return make_response(jsonify(attribute.json()), 200)
        return make_response(jsonify({"message": "DefGlobalConditionLogicAttribute not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving condition logic attribute", "error": str(e)}), 500)
    

@flask_app.route('/def_global_condition_logic_attributes/<int:page>/<int:limit>', methods=['GET'])
# @jwt_required()
def get_paginated_def_global_condition_logic_attributes(page, limit):
    try:
        query = DefGlobalConditionLogicAttribute.query.order_by(DefGlobalConditionLogicAttribute.id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            'message': 'Error fetching global condition logic attributes',
            'error': str(e)
        }), 500)


@flask_app.route('/def_global_condition_logic_attributes/upsert', methods=['POST'])
def upsert_def_global_condition_logic_attributes():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []

        for data in data_list:
            id = data.get('id')
            def_global_condition_logic_id = data.get('def_global_condition_logic_id')
            widget_position = data.get('widget_position')
            widget_state = data.get('widget_state')

            existing_attr = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()

            if existing_attr:
                # Prevent changing foreign key
                if def_global_condition_logic_id and def_global_condition_logic_id != existing_attr.def_global_condition_logic_id:
                    response.append({
                        'id': id,
                        'status': 'error',
                        'message': 'Updating def_global_condition_logic_id is not allowed'
                    })
                    continue

                existing_attr.widget_position = widget_position
                existing_attr.widget_state = widget_state
                db.session.add(existing_attr)

                response.append({
                    'id': existing_attr.id,
                    'status': 'updated',
                    'message': 'Attribute updated successfully'
                })

            else:
                # Validate required FK
                if not def_global_condition_logic_id:
                    response.append({
                        'status': 'error',
                        'message': 'def_global_condition_logic_id is required for new records'
                    })
                    continue

                # Check foreign key existence
                logic_exists = db.session.query(
                    db.exists().where(DefGlobalConditionLogic.def_global_condition_logic_id == def_global_condition_logic_id)
                ).scalar()

                if not logic_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_global_condition_logic_id {def_global_condition_logic_id} does not exist'
                    })
                    continue

                if not id:
                    response.append({
                        'status': 'error',
                        'message': 'id is required for new records'
                    })
                    continue

                new_attr = DefGlobalConditionLogicAttribute(
                    id=id,
                    def_global_condition_logic_id=def_global_condition_logic_id,
                    widget_position=widget_position,
                    widget_state=widget_state
                )
                db.session.add(new_attr)
                db.session.flush()

                response.append({
                    'id': new_attr.id,
                    'status': 'created',
                    'message': 'Attribute created successfully'
                })

        db.session.commit()
        return make_response(jsonify(response), 200)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error during upsert'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error during upsert',
            'error': str(e)
        }), 500)


@flask_app.route('/def_global_condition_logic_attributes/<int:id>', methods=['PUT'])
def update_def_global_condition_logic_attribute(id):
    try:
        data = request.get_json()
        attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()

        if not attribute:
            return make_response(jsonify({'message': 'DefGlobalConditionLogicAttribute not found'}), 404)

        # Update allowed fields
        attribute.widget_position = data.get('widget_position', attribute.widget_position)
        attribute.widget_state = data.get('widget_state', attribute.widget_state)

        db.session.commit()

        return make_response(jsonify({'message': 'DefGlobalConditionLogicAttribute updated successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error updating DefGlobalConditionLogicAttribute',
            'error': str(e)
        }), 500)




@flask_app.route('/def_global_condition_logic_attributes/<int:id>', methods=['DELETE'])
def delete_def_global_condition_logic_attribute(id):
    try:
        attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()

        if not attribute:
            return make_response(jsonify({'message': 'DefGlobalConditionLogicAttribute not found'}), 404)

        db.session.delete(attribute)
        db.session.commit()

        return make_response(jsonify({'message': 'DefGlobalConditionLogicAttribute deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error deleting DefGlobalConditionLogicAttribute',
            'error': str(e)
        }), 500)










#Def_access_point_elements
@flask_app.route('/def_access_point_elements', methods=['POST'])
def create_def_access_point_element():
    try:
        def_data_source_id = request.json.get('def_data_source_id')
        element_name = request.json.get('element_name')
        description = request.json.get('description')
        platform = request.json.get('platform')
        element_type = request.json.get('element_type')
        access_control = request.json.get('access_control')
        change_control = request.json.get('change_control')
        audit = request.json.get('audit')
        created_by = request.json.get('created_by')
        last_updated_by = request.json.get('last_updated_by')

        if not def_data_source_id:
            return make_response(jsonify({'message': 'def_data_source_id is required'}), 400)

        data_source = DefDataSource.query.get(def_data_source_id)
        if not data_source:
            return make_response(jsonify({'message': 'Invalid def_data_source_id — referenced source not found'}), 404)

        new_element = DefAccessPointElement(
            def_data_source_id=def_data_source_id,
            element_name=element_name,
            description=description,
            platform=platform,
            element_type=element_type,
            access_control=access_control,
            change_control=change_control,
            audit=audit,
            created_by=created_by,
            last_updated_by=last_updated_by
        )

        db.session.add(new_element)
        db.session.commit()
        return make_response(jsonify({"message": "DefAccessPointElement created successfully!"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    

@flask_app.route('/def_access_point_elements', methods=['GET'])
def get_all_def_access_point_elements():
    try:
        elements = DefAccessPointElement.query.order_by(
            desc(DefAccessPointElement.def_access_point_id)
        ).all()
        return jsonify([element.json() for element in elements])
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500
    
@flask_app.route('/def_access_point_elements/<int:dap_id>', methods=['GET'])
def get_def_access_point_element_by_id(dap_id):
    try:
        element = DefAccessPointElement.query.get(dap_id)
        
        if element is None:
            return jsonify({"error": "Element not found"}), 404

        return jsonify(element.json())

    except ValueError:
        return jsonify({"error": "Invalid ID format. ID must be an integer."}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route('/def_access_point_elements/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_access_point_elements(page, limit):
    try:
        search_query = request.args.get('element_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAccessPointElement.query

        if search_query:
            query = query.filter(
                or_(
                    DefAccessPointElement.element_name.ilike(f'%{search_query}%'),
                    DefAccessPointElement.element_name.ilike(f'%{search_underscore}%'),
                    DefAccessPointElement.element_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAccessPointElement.def_access_point_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [element.json() for element in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching access point elements', 'error': str(e)}), 500)



@flask_app.route('/def_access_point_elements/<int:page>/<int:limit>', methods=['GET'])
def get_paginated_elements(page, limit):
    try:
        query = DefAccessPointElement.query.order_by(DefAccessPointElement.def_access_point_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        # Return paginated data
        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching elements', 'error': str(e)}), 500)

    
@flask_app.route('/def_access_point_elements/<int:dap_id>', methods=['PUT'])
def update_def_access_point_element(dap_id):
    try:
        element = DefAccessPointElement.query.filter_by(def_access_point_id=dap_id).first()
        if not element:
            return make_response(jsonify({'message': 'DefAccessPointElement not found'}), 404)

        new_data_source_id = request.json.get('def_data_source_id')

        # If a new def_data_source_id is provided, verify that it exists
        if new_data_source_id is not None:
            data_source_exists = DefDataSource.query.filter_by(def_data_source_id=new_data_source_id).first()
            if not data_source_exists:
                return make_response(jsonify({'message': 'Invalid def_data_source_id – no such data source exists'}), 400)
            element.def_data_source_id = new_data_source_id

        
        element.element_name = request.json.get('element_name', element.element_name)
        element.description = request.json.get('description', element.description)
        element.platform = request.json.get('platform', element.platform)
        element.element_type = request.json.get('element_type', element.element_type)
        element.access_control = request.json.get('access_control', element.access_control)
        element.change_control = request.json.get('change_control', element.change_control)
        element.audit = request.json.get('audit', element.audit)
        element.created_by = request.json.get('created_by', element.created_by)
        element.last_updated_by = request.json.get('last_updated_by', element.last_updated_by)

        db.session.commit()
        return make_response(jsonify({'message': 'DefAccessPointElement updated successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error updating DefAccessPointElement', 'error': str(e)}), 500)


@flask_app.route('/def_access_point_elements/<int:dap_id>', methods=['DELETE'])
def delete_element(dap_id):
    try:
        element = DefAccessPointElement.query.filter_by(def_access_point_id=dap_id).first()

        if element:
            db.session.delete(element)
            db.session.commit()
            return make_response(jsonify({'message': 'DefAccessPointElement deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'DefAccessPointElement not found'}), 404)

    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting DefAccessPointElement', 'error': str(e)}), 500)












#Def_Data_Sources
@flask_app.route('/def_data_sources', methods=['POST'])
def create_def_data_source():
    try:
        new_ds = DefDataSource(
            datasource_name=request.json.get('datasource_name'),
            description=request.json.get('description'),
            application_type=request.json.get('application_type'),
            application_type_version=request.json.get('application_type_version'),
            last_access_synchronization_date=request.json.get('last_access_synchronization_date'),
            last_access_synchronization_status=request.json.get('last_access_synchronization_status'),
            last_transaction_synchronization_date=request.json.get('last_transaction_synchronization_date'),
            last_transaction_synchronization_status=request.json.get('last_transaction_synchronization_status'),
            default_datasource=request.json.get('default_datasource'),
            created_by=request.json.get('created_by'),
            last_updated_by=request.json.get('last_updated_by')
        )
        db.session.add(new_ds)
        db.session.commit()
        return make_response(jsonify({'message': 'Data source created successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating data source', 'error': str(e)}), 500)


@flask_app.route('/def_data_sources', methods=['GET'])
@jwt_required()
def get_all_def_data_sources():
    try:
        data_sources = DefDataSource.query.order_by(DefDataSource.def_data_source_id.desc()).all()
        return make_response(jsonify([ds.json() for ds in data_sources]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching data sources', 'error': str(e)}), 500)

@flask_app.route('/def_data_sources/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_data_sources(page, limit):
    try:
        search_query = request.args.get('datasource_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefDataSource.query

        if search_query:
            query = query.filter(
                or_(
                    DefDataSource.datasource_name.ilike(f'%{search_query}%'),
                    DefDataSource.datasource_name.ilike(f'%{search_underscore}%'),
                    DefDataSource.datasource_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefDataSource.def_data_source_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [ds.json() for ds in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching data sources', 'error': str(e)}), 500)

@flask_app.route('/def_data_sources/<int:page>/<int:limit>', methods=['GET'])
def get_paginated_def_data_sources(page, limit):
    try:
        paginated = DefDataSource.query.order_by(DefDataSource.def_data_source_id.desc()).paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            'items': [ds.json() for ds in paginated.items],
            'total': paginated.total,
            'pages': paginated.pages,
            'page': paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching paginated data sources', 'error': str(e)}), 500)

@flask_app.route('/def_data_sources/<int:id>', methods=['GET'])
def get_def_data_source_by_id(id):
    try:
        ds = DefDataSource.query.filter_by(def_data_source_id=id).first()
        if ds:
            return make_response(jsonify(ds.json()), 200)
        return make_response(jsonify({'message': 'Data source not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching data source', 'error': str(e)}), 500)

@flask_app.route('/def_data_sources/<int:id>', methods=['PUT'])
def update_def_data_source(id):
    try:
        ds = DefDataSource.query.filter_by(def_data_source_id=id).first()
        if ds:
            ds.datasource_name = request.json.get('datasource_name', ds.datasource_name)
            ds.description = request.json.get('description', ds.description)
            ds.application_type = request.json.get('application_type', ds.application_type)
            ds.application_type_version = request.json.get('application_type_version', ds.application_type_version)
            ds.last_access_synchronization_date = request.json.get('last_access_synchronization_date', ds.last_access_synchronization_date)
            ds.last_access_synchronization_status = request.json.get('last_access_synchronization_status', ds.last_access_synchronization_status)
            ds.last_transaction_synchronization_date = request.json.get('last_transaction_synchronization_date', ds.last_transaction_synchronization_date)
            ds.last_transaction_synchronization_status = request.json.get('last_transaction_synchronization_status', ds.last_transaction_synchronization_status)
            ds.default_datasource = request.json.get('default_datasource', ds.default_datasource)
            ds.created_by = request.json.get('created_by', ds.created_by)
            ds.last_updated_by = request.json.get('last_updated_by', ds.last_updated_by)
            db.session.commit()
            return make_response(jsonify({'message': 'Data source updated successfully'}), 200)
        return make_response(jsonify({'message': 'Data source not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating data source', 'error': str(e)}), 500)


@flask_app.route('/def_data_sources/<int:id>', methods=['DELETE'])
def delete_def_data_source(id):
    try:
        ds = DefDataSource.query.filter_by(def_data_source_id=id).first()
        if ds:
            db.session.delete(ds)
            db.session.commit()
            return make_response(jsonify({'message': 'Data source deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Data source not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting data source', 'error': str(e)}), 500)











#def_access_entitlements
@flask_app.route('/def_access_entitlements', methods=['GET'])
def get_all_entitlements():
    try:
        entitlements = DefAccessEntitlement.query.order_by(DefAccessEntitlement.def_entitlement_id.desc()).all()
        return make_response(jsonify([e.json() for e in entitlements]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching entitlements', 'error': str(e)}), 500)


@flask_app.route('/def_access_entitlements/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_access_entitlements(page, limit):
    try:
        search_query = request.args.get('entitlement_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAccessEntitlement.query

        if search_query:
            query = query.filter(
                or_(
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_query}%'),
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_underscore}%'),
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAccessEntitlement.def_entitlement_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [e.json() for e in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching entitlements', 'error': str(e)}), 500)

@flask_app.route('/def_access_entitlements/<int:page>/<int:limit>', methods=['GET'])
def get_paginated_entitlements(page, limit):
    try:
        paginated = DefAccessEntitlement.query.order_by(DefAccessEntitlement.def_entitlement_id.desc()).paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            'items': [e.json() for e in paginated.items],
            'total': paginated.total,
            'pages': paginated.pages,
            'page': paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching paginated entitlements', 'error': str(e)}), 500)


@flask_app.route('/def_access_entitlements/<int:id>', methods=['GET'])
def get_entitlement_by_id(id):
    try:
        e = DefAccessEntitlement.query.filter_by(def_entitlement_id=id).first()
        if e:
            return make_response(jsonify(e.json()), 200)
        return make_response(jsonify({'message': 'Entitlement not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching entitlement', 'error': str(e)}), 500)


@flask_app.route('/def_access_entitlements', methods=['POST'])
def create_entitlement():
    try:
        new_e = DefAccessEntitlement(
            entitlement_name=request.json.get('entitlement_name'),
            description=request.json.get('description'),
            comments=request.json.get('comments'),
            status=request.json.get('status'),
            effective_date= datetime.utcnow().date(),
            revision= 0,
            revision_date= datetime.utcnow().date(),
            created_by=request.json.get('created_by'),
            last_updated_by=request.json.get('last_updated_by')
        )
        db.session.add(new_e)
        db.session.commit()
        return make_response(jsonify({'message': 'Entitlement created successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating entitlement', 'error': str(e)}), 500)


@flask_app.route('/def_access_entitlements/<int:id>', methods=['PUT'])
def update_entitlement(id):
    try:
        e = DefAccessEntitlement.query.filter_by(def_entitlement_id=id).first()
        if e:
            e.entitlement_name = request.json.get('entitlement_name', e.entitlement_name)
            e.description = request.json.get('description', e.description)
            e.comments = request.json.get('comments', e.comments)
            e.status = request.json.get('status', e.status)
            e.effective_date = datetime.utcnow().date()
            e.revision =  e.revision + 1
            e.revision_date = datetime.utcnow().date()
            e.created_by = request.json.get('created_by', e.created_by)
            e.last_updated_by = request.json.get('last_updated_by', e.last_updated_by)
            db.session.commit()
            return make_response(jsonify({'message': 'Entitlement updated successfully'}), 200)
        return make_response(jsonify({'message': 'Entitlement not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating entitlement', 'error': str(e)}), 500)


@flask_app.route('/def_access_entitlements/<int:id>', methods=['DELETE'])
def delete_entitlement(id):
    try:
        e = DefAccessEntitlement.query.filter_by(def_entitlement_id=id).first()
        if e:
            db.session.delete(e)
            db.session.commit()
            return make_response(jsonify({'message': 'Entitlement deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Entitlement not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting entitlement', 'error': str(e)}), 500)





#redis

@flask_app.route('/scheduled_tasks', methods=['GET'])
def get_redbeat_scheduled_tasks():
    try:
        tasks_output = []
        local_tz = ZoneInfo("Asia/Dhaka")

        def get_next_run_from_redis(redis_client, key_str, local_tz):
            score = redis_client.zscore("redbeat::schedule", key_str)
            if score is None:
                return None
            dt_utc = datetime.fromtimestamp(score, timezone.utc)
            dt_local = dt_utc.astimezone(local_tz)
            return {
                # "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S %Z"),
                # "local": dt_local.strftime("%Y-%m-%d %H:%M:%S %Z"),
                "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "local": dt_local.strftime("%A, %d %B %Y, %I:%M %p %Z (%z)")
            }

        for key_str in redis_client.scan_iter("redbeat:*"):
            if key_str == "redbeat::schedule":
                continue  # skip internal RedBeat key
            key_type = redis_client.type(key_str)
            # if key_type != "hash":
            #     continue  # skip non-hash keys


            if key_type != "hash":
                print(f"Skipping key (not hash): {key_str} -> type: {key_type}")
                continue

            # ttl = redis_client.ttl(key_str)
            # if ttl == -2:
            #     continue  # key has expired

            task_data = redis_client.hgetall(key_str)
            if not task_data:
                continue

            # task_data is dict[str, str] due to decode_responses=True

            definition_raw = task_data.get("definition", "{}")
            try:
                definition = json.loads(definition_raw)
            except json.JSONDecodeError:
                definition = {}



            # try:
            #     definition = json.loads(task_data.get("definition", "{}"))
            # except json.JSONDecodeError:
            #     definition = {}

            # task_name = definition.get("name") or key_str.split(":", 1)[-1]
            def clean_task_name(name):
                # Remove trailing underscore + UUID pattern, e.g. _e43d6b82-5ad3-47e0-b215-ff9912a7f6d8
                return re.sub(r'_[0-9a-fA-F-]{36}$', '', name)

            raw_name = definition.get("name") or key_str.split(":", 1)[-1]
            task_name = clean_task_name(raw_name)
    

            # Exclude tasks starting with celery.
            task_full_name = definition.get("task", "")
            if task_full_name.startswith("celery."):
                continue

            # Parse meta info
            try:
                meta_raw = json.loads(task_data.get('meta', '{}')) or {}
                last_run_at = meta_raw.get("last_run_at", {})
                total_run_count = meta_raw.get("total_run_count", 0)
                

                

                if all(k in last_run_at for k in ("year", "month", "day", "hour", "minute", "second")):
                    dt_utc = datetime(
                        year=last_run_at["year"],
                        month=last_run_at["month"],
                        day=last_run_at["day"],
                        hour=last_run_at["hour"],
                        minute=last_run_at["minute"],
                        second=last_run_at["second"],
                        microsecond=last_run_at.get("microsecond", 0),
                        tzinfo=timezone.utc
                    )
                    dt_local = dt_utc.astimezone(local_tz)
                    last_run = {
                        # "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S %Z"),
                        # "local": dt_local.strftime("%Y-%m-%d %H:%M:%S %Z")
                        "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "local": dt_local.strftime("%A, %d %B %Y, %I:%M %p %Z (%z)")
                    }
                else:
                    last_run = None

            except Exception:
                last_run = None
                total_run_count = 0

            # Get next run from Redis sorted set score
            next_run = get_next_run_from_redis(redis_client, key_str, local_tz)

            # Identify interval or cron schedule
            schedule_info = {}
            if "schedule" in definition:
                schedule = definition["schedule"]
                # if "every" in schedule:
                if isinstance(schedule, dict) and "every" in schedule:
                    schedule_info["type"] = "interval"
                    schedule_info["every"] = schedule["every"]
                elif any(k in schedule for k in ["minute", "hour", "day_of_week", "day_of_month", "month_of_year"]):
                    schedule_info["type"] = "crontab"
                    schedule_info["expression"] = schedule
                else:
                    schedule_info["type"] = "unknown"
            else:
                schedule_info["type"] = "immediate"

            if next_run is None:
                schedule_info["status"] = "inactive"
            else:
            # Parse the UTC datetime string
                try:
                    next_run_utc = datetime.strptime(next_run["utc"], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
                    now_utc = datetime.now(timezone.utc)

                    if next_run_utc <= now_utc:
                        schedule_info["status"] = "expired"
                    else:
                        schedule_info["status"] = "active"
                except Exception:
                    schedule_info["status"] = "unknown"

            tasks_output.append({
                "task_key": key_str,
                "task_name": task_name,
                "schedule_type": schedule_info["type"],
                "schedule_status": schedule_info.get("status"),
                "schedule_details": schedule_info.get("expression") or {"every": schedule_info.get("every")},
                "next_run": next_run,
                "last_run": last_run,
                "total_run_count": total_run_count,
            })

        return jsonify({
            "total_tasks": len(tasks_output),
            "tasks": tasks_output
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@flask_app.route('/scheduled_tasks/<path:task_key>', methods=['GET'])
def get_redbeat_scheduled_task(task_key):
    try:
        local_tz = ZoneInfo("Asia/Dhaka")
        redis_key = task_key if task_key.startswith("redbeat:") else f"redbeat:{task_key}"

        if redis_key == "redbeat::schedule":
            return jsonify({"error": "Invalid task key"}), 400

        if not redis_client.exists(redis_key):
            return jsonify({"error": "Task not found"}), 404

        task_type = redis_client.type(redis_key)
        if task_type != "hash":
            return jsonify({"error": "Invalid task format"}), 400

        task_data = redis_client.hgetall(redis_key)
        if not task_data:
            return jsonify({"error": "Task data missing or empty"}), 404

        try:
            definition = json.loads(task_data.get("definition", "{}"))
        except json.JSONDecodeError:
            definition = {}

        def clean_task_name(name):
            return re.sub(r'_[0-9a-fA-F-]{36}$', '', name)

        raw_name = definition.get("name") or redis_key.split(":", 1)[-1]
        task_name = clean_task_name(raw_name)

        if definition.get("task", "").startswith("celery."):
            return jsonify({"error": "Internal Celery task, not user-defined"}), 400

        # Parse meta info
        try:
            meta_raw = json.loads(task_data.get("meta", '{}')) or {}
            last_run_at = meta_raw.get("last_run_at", {})
            total_run_count = meta_raw.get("total_run_count", 0)

            if all(k in last_run_at for k in ("year", "month", "day", "hour", "minute", "second")):
                dt_utc = datetime(
                    year=last_run_at["year"],
                    month=last_run_at["month"],
                    day=last_run_at["day"],
                    hour=last_run_at["hour"],
                    minute=last_run_at["minute"],
                    second=last_run_at["second"],
                    microsecond=last_run_at.get("microsecond", 0),
                    tzinfo=timezone.utc
                )
                dt_local = dt_utc.astimezone(local_tz)
                last_run = {
                    "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "local": dt_local.strftime("%A, %d %B %Y, %I:%M %p %Z (%z)")
                }
            else:
                last_run = None

        except Exception:
            last_run = None
            total_run_count = 0

        # Get next run
        def get_next_run_from_redis(redis_client, key_str, local_tz):
            score = redis_client.zscore("redbeat::schedule", key_str)
            if score is None:
                return None
            dt_utc = datetime.fromtimestamp(score, timezone.utc)
            dt_local = dt_utc.astimezone(local_tz)
            return {
                "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "local": dt_local.strftime("%A, %d %B %Y, %I:%M %p %Z (%z)")
            }

        next_run = get_next_run_from_redis(redis_client, redis_key, local_tz)

        schedule_info = {}
        if "schedule" in definition:
            schedule = definition["schedule"]
            if isinstance(schedule, dict) and "every" in schedule:
                schedule_info["type"] = "interval"
                schedule_info["every"] = schedule["every"]
            elif any(k in schedule for k in ["minute", "hour", "day_of_week", "day_of_month", "month_of_year"]):
                schedule_info["type"] = "crontab"
                schedule_info["expression"] = schedule
            else:
                schedule_info["type"] = "unknown"
        else:
            schedule_info["type"] = "immediate"

        schedule_info["status"] = "active" if next_run else "inactive"

        return jsonify({
            "task_key": redis_key,
            "task_name": task_name,
            "schedule_type": schedule_info["type"],
            "schedule_status": schedule_info.get("status"),
            "schedule_details": schedule_info.get("expression") or {"every": schedule_info.get("every")},
            "next_run": next_run,
            "last_run": last_run,
            "total_run_count": total_run_count,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500







#DEF_CONTROLS
@flask_app.route('/def_controls', methods=['GET'])
@jwt_required()
def get_all_controls():
    try:
        controls = DefControl.query.order_by(DefControl.def_control_id.desc()).all()
        return make_response(jsonify([c.json() for c in controls]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching controls', 'error': str(e)}), 500)



@flask_app.route('/def_controls/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_controls(page, limit):
    try:
        paginated = DefControl.query.order_by(DefControl.def_control_id.desc()).paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            'items': [control.json() for control in paginated.items],
            'total': paginated.total,
            'pages': paginated.pages,
            'page': paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching paginated controls', 'error': str(e)}), 500)



@flask_app.route('/def_controls/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_controls(page, limit):
    try:
        search_query = request.args.get('control_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefControl.query

        if search_query:
            query = query.filter(
                or_(
                    DefControl.control_name.ilike(f'%{search_query}%'),
                    DefControl.control_name.ilike(f'%{search_underscore}%'),
                    DefControl.control_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefControl.def_control_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [control.json() for control in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page":  1 if paginated.total == 0 else paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching controls', 'error': str(e)}), 500)


@flask_app.route('/def_controls/<int:control_id>', methods=['GET'])
@jwt_required()
def get_control_by_id(control_id):
    try:
        control = DefControl.query.filter_by(def_control_id=control_id).first()
        if control:
            return make_response(jsonify(control.json()), 200)
        return make_response(jsonify({'message': 'Control not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching control', 'error': str(e)}), 500)


@flask_app.route('/def_controls', methods=['POST'])
@jwt_required()
def create_control():
    try:
        new_control = DefControl(
            control_name=request.json.get('control_name'),
            description=request.json.get('description'),
            pending_results_count=request.json.get('pending_results_count'),
            control_type=request.json.get('control_type'),
            priority=request.json.get('priority'),
            datasources=request.json.get('datasources'),
            last_run_date=request.json.get('last_run_date'),
            last_updated_date=request.json.get('last_updated_date'),
            status=request.json.get('status'),
            state=request.json.get('state'),
            result_investigator=request.json.get('result_investigator'),
            authorized_data=request.json.get('authorized_data'),
            revision=0,
            revision_date=datetime.utcnow().date(),
            created_by = get_jwt_identity(),
            created_date=datetime.utcnow().date()
        )
        db.session.add(new_control)
        db.session.commit()
        return make_response(jsonify({'message': 'Control created successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating control', 'error': str(e)}), 500)

@flask_app.route('/def_controls/<int:control_id>', methods=['PUT'])
@jwt_required()
def update_control(control_id):
    try:
        control = DefControl.query.filter_by(def_control_id=control_id).first()
        if control:
            control.control_name = request.json.get('control_name', control.control_name)
            control.description = request.json.get('description', control.description)
            control.pending_results_count = request.json.get('pending_results_count', control.pending_results_count)
            control.control_type = request.json.get('control_type', control.control_type)
            control.priority = request.json.get('priority', control.priority)
            control.datasources = request.json.get('datasources', control.datasources)
            control.last_run_date = request.json.get('last_run_date', control.last_run_date)
            control.last_updated_date = request.json.get('last_updated_date', control.last_updated_date)
            control.status = request.json.get('status', control.status)
            control.state = request.json.get('state', control.state)
            control.result_investigator = request.json.get('result_investigator', control.result_investigator)
            control.authorized_data = request.json.get('authorized_data', control.authorized_data)
            control.revision += 1
            control.revision_date = datetime.utcnow().date()
            control.created_by = get_jwt_identity()
            control.created_date = request.json.get('created_date', control.created_date)

            db.session.commit()
            return make_response(jsonify({'message': 'Control updated successfully'}), 200)
        return make_response(jsonify({'message': 'Control not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating control', 'error': str(e)}), 500)


@flask_app.route('/def_controls/<int:control_id>', methods=['DELETE'])
@jwt_required()
def delete_control(control_id):
    try:
        control = DefControl.query.filter_by(def_control_id=control_id).first()
        if control:
            db.session.delete(control)
            db.session.commit()
            return make_response(jsonify({'message': 'Control deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Control not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting control', 'error': str(e)}), 500)





if __name__ == "__main__":
    flask_app.run(debug=True)
