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
import ast
import math
from redis import Redis
from requests.auth import HTTPBasicAuth
from zoneinfo import ZoneInfo
from itertools import count
from functools import wraps
from flask_cors import CORS 
from dotenv import load_dotenv            # To load environment variables from a .env file
from celery.schedules import crontab
from celery.result import AsyncResult      # For checking the status of tasks
from redbeat import RedBeatSchedulerEntry
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine, Text, desc, cast, TIMESTAMP, func, or_, text
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, make_response       # Flask utilities for handling requests and responses
from itsdangerous import BadSignature,SignatureExpired, URLSafeTimedSerializer
from flask_mail import Message as MailMessage
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity, decode_token
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
    DefAccessPoint,
    DefAccessPointsV,
    DefDataSource,
    DefAccessEntitlement,
    DefControl,
    DefActionItem,
    DefActionItemsV,
    DefActionItemAssignment,
    DefAlert,
    DefAlertRecipient,
    DefProcess,
    DefControlEnvironment,
    NewUserInvitation,
    DefJobTitle,
    DefAccessEntitlementElement,
    DefNotifications,
    DefRoles,
    DefUserGrantedRole,
    DefPrivilege,
    DefUserGrantedPrivilege,
    DefApiEndpoint,
    DefApiEndpointRole
)
from redbeat_s.red_functions import create_redbeat_schedule, update_redbeat_schedule, delete_schedule_from_redis
from ad_hoc.ad_hoc_functions import execute_ad_hoc_task, execute_ad_hoc_task_v1
from config import redis_url

# from workflow_engine.engine_v1 import run_workflow
# from workflow_engine import run_workflow
# from workflow_engine.services import run_workflow

flower_url = flask_app.config["FLOWER_URL"]
# invitation_expire_time = flask_app.config["INV_EXPIRE_TIME"]
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



def current_timestamp():
    return datetime.now().strftime('%d-%m-%Y %H:%M:%S')


serializer = URLSafeTimedSerializer(
    flask_app.config["JWT_SECRET_KEY"],
    salt="invite-link"
)



def role_required():

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                current_user_id = get_jwt_identity()
                if not current_user_id:
                    return jsonify({"message": "Authentication required"}), 401


                #  Extract route pattern
                rule = request.url_rule.rule        #  "/def_users/<int:user_id>/<status>"
                method = request.method             #  "GET"

                parts = rule.strip("/").split("/")

                api_endpoint = "/" + parts[0]       # "/def_users"

                parameter1 = None
                parameter2 = None

                if len(parts) > 1:
                    p1 = parts[1]
                    if p1.startswith("<") and p1.endswith(">"):
                        parameter1 = p1[1:-1].split(":")[-1]   # remove int: or string: type


                if len(parts) > 2:
                    p2 = parts[2]
                    if p2.startswith("<") and p2.endswith(">"):
                        parameter2 = p2[1:-1].split(":")[-1]



                #  Fetch allowed roles for this user
                user_roles = DefUserGrantedRole.query.filter_by(user_id=current_user_id).all()
                role_ids = [ur.role_id for ur in user_roles]

                if not role_ids:
                    return jsonify({"message": "No roles assigned"}), 403

                allowed_mappings = DefApiEndpointRole.query.filter(
                    DefApiEndpointRole.role_id.in_(role_ids)
                ).all()

                allowed_api_endpoint_ids = [m.api_endpoint_id for m in allowed_mappings]

                if not allowed_api_endpoint_ids:
                    return jsonify({"message": "User has no API access roles"}), 403


                #  Match the stored API endpoint rule in DB
                endpoint = DefApiEndpoint.query.filter(
                    DefApiEndpoint.api_endpoint_id.in_(allowed_api_endpoint_ids),
                    DefApiEndpoint.api_endpoint == api_endpoint,
                    DefApiEndpoint.method == method,
                    DefApiEndpoint.parameter1 == parameter1,
                    DefApiEndpoint.parameter2 == parameter2
                ).first()

                if not endpoint:
                    return jsonify({
                        "message": f"Access denied."
                    }), 403

                #  Check privilege
                user_privileges = DefUserGrantedPrivilege.query.filter_by(
                    user_id=current_user_id
                ).all()

                privilege_ids = [up.privilege_id for up in user_privileges]

                if endpoint.privilege_id not in privilege_ids:
                    return jsonify({"message": "Privilege denied"}), 403

                #  Access Granted
                return fn(*args, **kwargs)

            except Exception as e:
                return make_response(jsonify({"message": str(e)}), 500)

        return wrapper
    return decorator



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
@flask_app.route('/def_tenants', methods=['POST'])
@jwt_required()
def create_tenant():
    try:
       data = request.get_json()
    #    tenant_id   = generate_tenant_id()  # Call the function to get the result
       tenant_name = data['tenant_name']
       existing_name = DefTenant.query.filter_by(tenant_name=tenant_name).first()
       if existing_name:
            return make_response(jsonify({"message": "Tenant name already exists"}), 400)


       new_tenant  = DefTenant(
            tenant_name = tenant_name,
            created_by     = get_jwt_identity(),
            creation_date  = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
           )
       db.session.add(new_tenant)
       db.session.commit()
       return make_response(jsonify({"message": "Added successfully"}), 201)
   
    except IntegrityError as e:
        return make_response(jsonify({"message": "Error creating Tenant", "error": "Tenant already exists"}), 409)
    except Exception as e:
        return make_response(jsonify({"message": "Error creating Tenant", "error": str(e)}), 500)

       

# Get all tenants


@flask_app.route('/def_tenants', methods=['GET'])
@jwt_required()
def get_tenants():
    try:
        tenants = DefTenant.query.order_by(
            DefTenant.tenant_id.desc()
        ).all()

        return make_response(jsonify({
            "result": [tenant.json() for tenant in tenants]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error getting Tenants",
            "error": str(e)
        }), 500)


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
        return make_response(jsonify({"message": "Error fetching tenants", "error": str(e)}), 500)


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
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
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
@flask_app.route('/def_tenants', methods=['PUT'])
@jwt_required()
def update_tenant():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        if not tenant_id:
            return make_response(jsonify({"message": "tenant_id query parameter is required"}), 400)

        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if tenant:
            data = request.get_json()
            tenant.tenant_name  = data['tenant_name']
            tenant.last_updated_by = get_jwt_identity()
            tenant.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({"message": "Edited successfully"}), 200)
        return make_response(jsonify({"message": "Tenant not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error updating Tenant", "error": str(e)}), 500)


# Delete a tenant
@flask_app.route('/def_tenants', methods=['DELETE'])
@jwt_required()
def delete_tenant():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        if not tenant_id:
            return make_response(jsonify({"message": "tenant_id query parameter is required"}), 400)

        user = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return make_response(jsonify({"message": "Deleted successfully"}), 200)
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
            enterprise_type=enterprise_type,
            created_by     = get_jwt_identity(),
            creation_date  = datetime.utcnow(),
            last_updated_by= get_jwt_identity(),
            last_update_date= datetime.utcnow()
        )

        db.session.add(new_enterprise)
        db.session.commit()
        return make_response(jsonify({"message": "Added successfully"}), 201)

    except IntegrityError:
        return make_response(jsonify({"message": f"Enterprise setup already exists for tenant ID {tenant_id}."}), 409)
    except Exception as e:
        return make_response(jsonify({"message": "Failed to add enterprise setup.", "error": str(e)}), 500)

# Create or update enterprise setup
@flask_app.route('/def_tenant_enterprise_setup', methods=['POST'])
@jwt_required()
def create_update_enterprise():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        if not tenant_id:
            return make_response(jsonify({"message": "tenant_id query parameter is required"}), 400)

        data = request.get_json()
        enterprise_name = data['enterprise_name']
        enterprise_type = data['enterprise_type']
        user_invitation_validity = data.get('user_invitation_validity', "1h")


        tenant_exists = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if not tenant_exists:
            return make_response(jsonify({"message": "Tenant does not exist"}), 400)
        
        existing_enterprise = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()
        existing_enterprise_name  = DefTenantEnterpriseSetup.query.filter_by(enterprise_name=enterprise_name).first()
        if existing_enterprise_name and (not existing_enterprise or existing_enterprise_name.tenant_id != tenant_id):
            return make_response(jsonify({"message": f"Enterprise name '{enterprise_name}' already exists."}), 409)





        if existing_enterprise:
            existing_enterprise.enterprise_name = enterprise_name
            existing_enterprise.enterprise_type = enterprise_type
            existing_enterprise.user_invitation_validity = user_invitation_validity
            existing_enterprise.last_updated_by = get_jwt_identity()
            existing_enterprise.last_update_date = datetime.utcnow()
            existing_enterprise.user_invitation_validity = user_invitation_validity
            message = "Edited successfully"

        else:
            new_enterprise = DefTenantEnterpriseSetup(
                tenant_id = tenant_id,
                enterprise_name = enterprise_name,
                enterprise_type = enterprise_type,
                user_invitation_validity = user_invitation_validity,
                created_by     = get_jwt_identity(),
                creation_date   = datetime.utcnow(),
                last_updated_by = get_jwt_identity(),
                last_update_date = datetime.utcnow()
            )

            db.session.add(new_enterprise)
            message = "Added successfully"

        db.session.commit()
        return make_response(jsonify({"message": message, "result": new_enterprise.json() if not existing_enterprise else existing_enterprise.json()}), 200)

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
# Get one enterprise setup by tenant_id
@flask_app.route('/def_tenant_enterprise_setup', methods=['GET'])
@jwt_required()
def get_enterprise():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        if not tenant_id:
            return make_response(jsonify({
                "message": "tenant_id query parameter is required"
            }), 400)

        setup = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()

        if setup:
            return make_response(jsonify({
                "result": setup.json()
            }), 200)

        return make_response(jsonify({
            "message": "Enterprise setup not found"
        }), 404)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving enterprise setup",
            "error": str(e)
        }), 500)


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
        return jsonify({"message": "Error fetching enterprises", "error": str(e)}), 500

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
            setup.user_invitation_validity = data.get('user_invitation_validity', setup.user_invitation_validity)
            setup.last_updated_by = get_jwt_identity()
            setup.last_update_date = datetime.utcnow()
            db.session.commit()
            return make_response(jsonify({"message": "Edited successfully"}), 200)
        return make_response(jsonify({"message": "Enterprise setup not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error Editing enterprise setup", "error": str(e)}), 500)


# Delete enterprise setup
@flask_app.route('/def_tenant_enterprise_setup', methods=['DELETE'])
@jwt_required()
def delete_enterprise():
    try:
        tenant_id = request.args.get('tenant_id', type=int)
        if not tenant_id:
            return make_response(jsonify({"message": "tenant_id query parameter is required"}), 400)

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
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching enterprises", "error": str(e)}), 500)


@flask_app.route('/job_titles', methods=['POST'])
@jwt_required()
def create_job_title():
    try:
        data = request.get_json()
        job_title_name = data.get('job_title_name')
        tenant_id = data.get('tenant_id')

        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if not tenant:
            return jsonify({"message": "Invalid tenant_id. Tenant not found."}), 404

        existing_title = DefJobTitle.query.filter_by(job_title_name=job_title_name, tenant_id=tenant_id).first()
        if existing_title:
            return jsonify({"message": f"'{job_title_name}' already exists for this tenant."}), 409

        new_title = DefJobTitle(
            job_title_name   = job_title_name,
            tenant_id        = tenant_id,
            created_by       = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )
        db.session.add(new_title)
        db.session.commit()
        return jsonify({
            "message": "Added successfully",
            "job_title_id": new_title.job_title_id
        }), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Failed to create job title", "error": str(e)}), 500


@flask_app.route('/job_titles', methods=['GET'])
@jwt_required()
def get_job_titles():
    try:
        job_title_id = request.args.get('job_title_id', type=int)
        tenant_id = request.args.get('tenant_id', type=int)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int, default=10)

        # If job_title_id is provided → return single object
        if job_title_id:
            job = DefJobTitle.query.filter(DefJobTitle.job_title_id == job_title_id).first()
            if not job:
                return make_response(jsonify({"message": "Job title not found"}), 404)
            return make_response(jsonify(job.json()), 200)

        # Base query
        query = DefJobTitle.query

        # Filter by tenant if provided
        if tenant_id:
            query = query.filter(DefJobTitle.tenant_id == tenant_id)

        # Always order by id descending
        query = query.order_by(DefJobTitle.job_title_id.desc())

        # Pagination if page parameter provided
        if page:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "items": [item.json() for item in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)

        # No pagination → return all matching items
        items = query.all()
        if not items:
            return make_response(jsonify({"message": "No job titles found"}), 404)
        return make_response(jsonify([item.json() for item in items]), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Failed to retrieve job titles",
            "error": str(e)
        }), 500)




@flask_app.route('/job_titles', methods=['PUT'])
@jwt_required()
def update_job_title():
    try:
        job_title_id = request.args.get('job_title_id', type=int)
        if not job_title_id:
            return make_response(jsonify({"message": "Missing query parameter: job_title_id"}), 400)

        title = db.session.get(DefJobTitle, job_title_id)
        if not title:
            return make_response(jsonify({"message": "Job title not found"}), 404)

        data = request.get_json()
        if not data:
            return make_response(jsonify({"message": "Missing JSON body"}), 400)

        new_job_title_name = data.get('job_title_name', title.job_title_name)
        new_tenant_id = data.get('tenant_id', title.tenant_id)

        # Duplicate check — only if name or tenant is changing
        existing_title = (
            DefJobTitle.query.filter_by(job_title_name=new_job_title_name, tenant_id=new_tenant_id)
            .filter(DefJobTitle.job_title_id != job_title_id)  # exclude current record
            .first()
        )
        if existing_title:
            return jsonify({
                "message": f"'{new_job_title_name}' already exists for this tenant."
            }), 409


        title.job_title_name   = new_job_title_name
        title.tenant_id        = new_tenant_id
        title.last_updated_by  = get_jwt_identity()
        title.last_update_date = datetime.utcnow()
        db.session.commit()

        return jsonify({
            "message": "Edited successfully",
            "job_title_id": title.job_title_id
        }), 200

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "message": "Failed to update job title",
            "error": str(e)
        }), 500)



@flask_app.route('/job_titles', methods=['DELETE'])
@jwt_required()
def delete_job_title():
    try:
        job_title_id = request.args.get('job_title_id', type=int)
        if not job_title_id:
            return make_response(jsonify({"message": "Missing query parameter: job_title_id"}), 400)

        title = db.session.get(DefJobTitle, job_title_id)
        if not title:
            return make_response(jsonify({"message": "Job title not found"}), 404)

        db.session.delete(title)
        db.session.commit()
        return jsonify({"message": "Deleted successfully"}), 200

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "message": "Failed to delete job title",
            "error": str(e)
        }), 500)



# DELETE ALL TENANT

@flask_app.route('/tenants/cascade_delete', methods=['DELETE'])
@jwt_required()
def delete_tenant_and_related():
    try:
        tenant_id = request.args.get('tenant_id', type=int)

        if not tenant_id:
            return jsonify({"message": "Missing required query parameter: tenant_id"}), 400

        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if not tenant:
            return jsonify({"message": "Tenant not found"}), 404

        # Delete related records manually
        DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).delete()
        DefJobTitle.query.filter_by(tenant_id=tenant_id).delete()

        db.session.delete(tenant)
        db.session.commit()

        return jsonify({
            "message": f"Deleted successfully"
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({
            "message": "Failed to delete tenant and related data",
            "error": str(e)
        }), 500



@flask_app.route('/defusers', methods=['POST'])
@jwt_required()
def create_def_user():
    try:
        # Parse data from the request body
        data = request.get_json()
        # user_id         = generate_user_id()
        user_name       = data['user_name']
        user_type       = data['user_type']
        email_address   = data['email_address']
        tenant_id       = data['tenant_id']
        profile_picture = data.get('profile_picture') or {
            "original": "uploads/profiles/default/profile.jpg",
            "thumbnail": "uploads/profiles/default/thumbnail.jpg"
        }
        user_invitation_id = data.get('user_invitation_id')
        date_of_birth     = data.get('date_of_birth')

        # Duplicate check
        existing_user = DefUser.query.filter_by(email_address=email_address).first()
        if existing_user:
            return make_response(jsonify({"message": "Email address already exists"}), 409)
        

       # Convert the list of email addresses to a JSON-formatted string
       # email_addresses_json = json.dumps(email_addresses)  # Corrected variable name

       # Create a new ArcUser object
        new_user = DefUser(
        #   user_id         = user_id,
          user_name       = user_name,
          user_type       = user_type,
          email_address   = email_address,  # Corrected variable name
          created_by      = get_jwt_identity(),
          creation_date   = datetime.utcnow(),
          last_updated_by = get_jwt_identity(),
          last_update_date= datetime.utcnow(),
          tenant_id       = tenant_id,
          profile_picture = profile_picture,
          user_invitation_id = user_invitation_id,
          date_of_birth   = date_of_birth
        )
        # Add the new user to the database session
        db.session.add(new_user)
        # Commit the changes to the database
        db.session.commit()

        # Return a success response
        return make_response(jsonify({"message": "Added successfully",
                                       "User Id": new_user.user_id}), 201)

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    


@flask_app.route('/defusers', methods=['GET'])
@jwt_required()
def get_users():
    try:
        users = DefUser.query.all()
        return make_response(jsonify([user.json() for user in users]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting users', 'error': str(e)}), 500)
    

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
        return make_response(jsonify({'message': 'Error getting users', 'error': str(e)}), 500)



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
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching users", "error": str(e)}), 500)


# get a user by id
@flask_app.route('/defusers/<int:user_id>', methods=['GET'])
@jwt_required()
def get_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            return make_response(jsonify({'user': user.json()}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting user', 'error': str(e)}), 500)
    
    
@flask_app.route('/defusers/<int:user_id>', methods=['PUT'])
@jwt_required()
def update_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            data = request.get_json()
            if 'user_name' in data:
                user.user_name = data['user_name']
            if 'email_address' in data:
                user.email_address = data['email_address']
            if 'tenant_id' in data:
                user.tenant_id = data['tenant_id']
            if 'date_of_birth' in data:
                user.date_of_birth = data['date_of_birth']
            user.last_updated_by = get_jwt_identity()
            user.last_update_date = datetime.utcnow()
            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating user', 'error': str(e)}), 500)


@flask_app.route('/defusers/<int:user_id>', methods=['DELETE'])
@jwt_required()
def delete_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except:
        return make_response(jsonify({'message': 'Error deleting user'}), 500)



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
        return make_response(jsonify({'message': 'Error fetching users', 'error': str(e)}), 500)
        

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
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching users', 'error': str(e)}), 500)


@flask_app.route('/defpersons', methods=['POST'])
@jwt_required()
def create_arc_person():
    try:
        data = request.get_json()
        user_id     = data['user_id']
        first_name  = data['first_name']
        middle_name = data['middle_name']
        last_name   = data['last_name']
        job_title_id = data['job_title_id']  
        
        # create arc persons object 
        person =  DefPerson(
            user_id          = user_id,
            first_name       = first_name,
            middle_name      = middle_name,
            last_name        = last_name,
            job_title_id     = job_title_id,
            created_by       = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        ) 
        
        # Add arc persons data to the database session
        db.session.add(person)
        # Commit the changes to the database
        db.session.commit()
        # Return a success response
        return make_response(jsonify({"message": "Added successfully"}), 201)
    
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    
    
@flask_app.route('/defpersons', methods=['GET'])
@jwt_required()
def get_persons():
    try:
        persons = DefPerson.query.all()
        return make_response(jsonify([person.json() for person in persons]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting persons', 'error': str(e)}), 500)
    



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
        return make_response(jsonify({'message': 'Error getting persons', 'error': str(e)}), 500)

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
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching persons", "error": str(e)}), 500)


@flask_app.route('/defpersons/<int:user_id>', methods=['GET'])
@jwt_required()
def get_person(user_id):
    try:
        person = DefPerson.query.filter_by(user_id=user_id).first()
        if person:
            return make_response(jsonify({'person': person.json()}), 200)
        return make_response(jsonify({'message': 'Person not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting person', 'error': str(e)}), 500) 


@flask_app.route('/defpersons/<int:user_id>', methods=['PUT'])
@jwt_required()
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
            if 'job_title_id' in data:
                person.job_title_id = data['job_title_id']
            person.last_updated_by = get_jwt_identity()
            person.last_udpate_date = datetime.utcnow()
            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Person not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing person', 'error': str(e)}), 500)
    
    
@flask_app.route('/defpersons/<int:user_id>', methods=['DELETE'])
@jwt_required()
def delete_person(user_id):
    try:
        person = DefPerson.query.filter_by(user_id=user_id).first()
        if person:
            db.session.delete(person)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Person not found'}), 404)
    except:
        return make_response(jsonify({'message': 'Error deleting user'}), 500)


    
@flask_app.route('/def_user_credentials', methods=['POST'])
@jwt_required()
def create_user_credential():
    try:
        data = request.get_json()
        user_id = data['user_id']
        password = data['password']

        hashed_password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)

        credential = DefUserCredential(
            user_id          = user_id,
            password         = hashed_password,
            created_by       = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(credential)
        db.session.commit()

        return make_response(jsonify({"message": "Added successfully!"}), 201)

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    


    
    
@flask_app.route('/reset_user_password', methods=['PUT'])
@jwt_required()
def reset_user_password():
    try:
        data = request.get_json()
        current_user_id = data['user_id']
        old_password = data['old_password']
        new_password = data['new_password']

        user = DefUserCredential.query.get(current_user_id)
        if not user:
            return jsonify({'message': 'User not found'}), 404

        if not check_password_hash(user.password, old_password):
            return jsonify({'message': 'Invalid old password'}), 401

        hashed_new_password   = generate_password_hash(new_password, method='pbkdf2:sha256', salt_length=16)
        user.password         = hashed_new_password
        user.last_update_date = datetime.utcnow()
        user.last_updated_by  = get_jwt_identity()

        db.session.commit()

        return jsonify({'message': 'Edited successfully'}), 200

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)


@flask_app.route('/def_user_credentials/<int:user_id>', methods=['DELETE'])
@jwt_required()
def delete_user_credentials(user_id):
    try:
        credential = DefUserCredential.query.filter_by(user_id=user_id).first()
        if credential:
            db.session.delete(credential)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
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
@jwt_required()
def register_user():
    try:
        data = request.get_json()
        # Extract user fields
        # user_id         = generate_user_id()
        user_name       = data['user_name']
        user_type       = data['user_type']
        email_address   = data['email_address']
        created_by      = get_jwt_identity()
        last_updated_by = get_jwt_identity()
        tenant_id       = data['tenant_id']
        date_of_birth   = data.get('date_of_birth')
        # Extract person fields
        first_name      = data.get('first_name')
        middle_name     = data.get('middle_name')
        last_name       = data.get('last_name')
        job_title_id       = data.get('job_title_id')
        user_invitation_id = data.get('user_invitation_id')
        # Extract credentials
        password        = data['password']

        # Set default profile picture if not provided
        profile_picture = data.get('profile_picture') or {
            "original": "uploads/profiles/default/profile.jpg",
            "thumbnail": "uploads/profiles/default/thumbnail.jpg"
        }

        # Check for existing user/email
        # if DefUser.query.filter_by(user_name=user_name).first():
        #     return jsonify({"message": "Username already exists"}), 409
        # for email in email_address:
        #     if DefUser.query.filter(DefUser.email_address.contains    ([email])).first():
        #         return jsonify({"message": "Email already exists"}), 409

        # Check for existing username
        if DefUser.query.filter_by(user_name=user_name).first():
            return jsonify({"message": "Username already exists"}), 409

        # Check for existing email
        if DefUser.query.filter(DefUser.email_address == email_address).first():
            return jsonify({"message": "Email already exists"}), 409


        # Create user
        new_user = DefUser(
            # user_id         = user_id,
            user_name          = user_name,
            user_type          = user_type,
            email_address      = email_address,
            created_by         = created_by,
            creation_date      = datetime.utcnow(),
            last_updated_by    = last_updated_by,
            last_update_date   = datetime.utcnow(),
            tenant_id          = tenant_id,
            profile_picture    = profile_picture,
            user_invitation_id = user_invitation_id,
            date_of_birth      = date_of_birth
        )
        db.session.add(new_user)
        db.session.flush()

        # Create person if user_type is person
        if user_type.lower() == "person" :
            new_person = DefPerson(
                user_id          = new_user.user_id,
                first_name       = first_name,
                middle_name      = middle_name,
                last_name        = last_name,
                job_title_id     = job_title_id,
                created_by       = created_by,
                creation_date    = datetime.utcnow(),
                last_updated_by  = last_updated_by,
                last_update_date = datetime.utcnow()
            )
            db.session.add(new_person)


        
        # Create credentials
        hashed_password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)
        new_cred = DefUserCredential(
            user_id          = new_user.user_id,
            password         = hashed_password,
            created_by       = created_by,
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow()


        )
        db.session.add(new_cred)

        if user_invitation_id:  
            user_invitation = NewUserInvitation.query.filter_by(user_invitation_id=user_invitation_id).first()
            if user_invitation:

                user_invitation.registered_user_id = new_user.user_id
                user_invitation.status             = "ACCEPTED"
                user_invitation.accepted_at        = datetime.utcnow()
        

        db.session.commit()
        return jsonify({"message": "Added successfully", "user_id": new_user.user_id}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Registration failed", "error": str(e)}), 500

@flask_app.route('/users', methods=['GET'])
@jwt_required()
def defusers():
    try:
        defusers = DefUsersView.query.order_by(DefUsersView.user_id.desc()).all()
        return make_response(jsonify([defuser.json() for defuser in defusers]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting users', 'error': str(e)}), 500)
    
    
@flask_app.route('/users/<int:user_id>', methods=['GET'])
@jwt_required()
def get_specific_user(user_id):
    try:
        user = DefUsersView.query.filter_by(user_id=user_id).first()
        if user:
            return make_response(jsonify(user.json()), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting User', 'error': str(e)}), 500)  
    

@flask_app.route('/users/<int:user_id>', methods=['PUT'])
@jwt_required()
def update_specific_user(user_id):
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({'message': 'No input data provided'}), 400)

        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({'message': 'User not found'}), 404)
        

        # --- Username & Email uniqueness check ---
        new_user_name     = data.get('user_name')
        new_email_address = data.get('email_address')
        new_tenant_id     = data.get('tenant_id')
        new_date_of_birth = data.get('date_of_birth')

        if new_user_name or new_email_address:
            conflict_user = DefUser.query.filter(
                ((DefUser.user_name == new_user_name) | (DefUser.email_address == new_email_address)),
                DefUser.user_id != user_id
            ).first()
            if conflict_user:
                return make_response(jsonify({'message': 'Username or email already exists for another user'}), 400)

         # Update username/email after uniqueness check
        if new_user_name:
            user.user_name = new_user_name
        if new_email_address:
            user.email_address = new_email_address
        if new_tenant_id:
            user.tenant_id = new_tenant_id
        if new_date_of_birth:
            user.date_of_birth = new_date_of_birth

        # Update DefUser fields
        user.last_update_date = datetime.utcnow()
        user.last_updated_by  = get_jwt_identity()

        # Update DefPerson fields if user_type is "person"
        if user.user_type and user.user_type.lower() == "person":
            person = DefPerson.query.filter_by(user_id=user_id).first()
            if not person:
                return make_response(jsonify({'message': 'Person not found'}), 404)

            person.first_name       = data.get('first_name', person.first_name)
            person.middle_name      = data.get('middle_name', person.middle_name)
            person.last_name        = data.get('last_name', person.last_name)
            person.job_title_id     = data.get('job_title_id', person.job_title_id)
            person.last_update_date = datetime.utcnow()
            person.last_updated_by  = get_jwt_identity()
    

        # Password update logic
        password = data.get('password')
        if password:
            user_cred = DefUserCredential.query.filter_by(user_id=user_id).first()
            if not user_cred:
                return make_response(jsonify({'message': 'User credentials not found'}), 404)

            user_cred.password        = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)
            user_cred.last_update_date= datetime.utcnow()
            user_cred.last_updated_by = get_jwt_identity()

        db.session.commit()
        return make_response(jsonify({'message': 'Edited successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error updating user', 'error': str(e)}), 500)



@flask_app.route('/users/<int:user_id>', methods=['DELETE'])
@jwt_required()
def delete_specific_user(user_id):
    try:
        # Find the user record in the DefUser table
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({'message': 'User not found'}), 404)

        access_profiles = DefAccessProfile.query.filter_by(user_id=user_id).all()
        for profile in access_profiles:
            db.session.delete(profile)


        if user.user_type and user.user_type.lower() == "person":
            person = DefPerson.query.filter_by(user_id=user_id).first()
            if person:
                db.session.delete(person)

        user_credential = DefUserCredential.query.filter_by(user_id=user_id).first()
        if user_credential:
            db.session.delete(user_credential)


        db.session.delete(user)
        db.session.commit()

        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error deleting user',
            'error': str(e)
        }), 500)



@flask_app.route('/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        user = data.get('user', '').strip()
        password = data.get('password')

        if not user or not password:
            return jsonify({"message": "Email/Username and Password are required."}), 400

        user_record = DefUser.query.filter(
            (DefUser.email_address.ilike(f"%{user}%")) |
            (DefUser.user_name == user)
        ).first()

        access_profile = DefAccessProfile.query.filter(
            func.trim(DefAccessProfile.profile_id).ilike(f"%{user}%"),
            func.trim(DefAccessProfile.profile_type).ilike("Email")
        ).first()

        user_id = None
        if user_record:
            user_id = user_record.user_id
        elif access_profile:
            user_id = access_profile.user_id

        if not user_id:
            return jsonify({"message": "User not found."}), 404

        user_cred = DefUserCredential.query.filter_by(user_id=user_id).first()
        if not user_cred:
            return jsonify({"message": "User credentials not found."}), 404

        if not check_password_hash(user_cred.password, password):
            return jsonify({"message": "Invalid email/username or password."}), 401


        access_token = create_access_token(identity=str(user_id))

        return jsonify({
            "isLoggedIn": True,
            "user_id": user_id,
            "access_token": access_token
        }), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500




@flask_app.route('/access_profiles/<int:user_id>', methods=['POST'])
@jwt_required()
def create_access_profiles(user_id):
    try:
        profile_type = request.json.get('profile_type') # Fixed incorrect key
        profile_id = request.json.get('profile_id')
        primary_yn = request.json.get('primary_yn', 'N') # Default to 'N' if not provided


        if not profile_type or not profile_id:
            return make_response(jsonify({"message": "Missing required fields"}), 400)

        # Check if user_id exists in def_users
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({"message": f"User with ID {user_id} not found in def_users"}), 404)

        # Check if profile_id exists in DefAccessProfile or DefUser.email_address (case-insensitive)
        existing_profile = DefAccessProfile.query.filter(func.lower(DefAccessProfile.profile_id) == profile_id.lower()).first()
        if existing_profile:
            return make_response(jsonify({"message": f"Email '{profile_id}' already exists in DefAccessProfile"}), 409)

        existing_user = DefUser.query.filter(func.lower(DefUser.email_address) == profile_id.lower()).first()
        if existing_user:
            return make_response(jsonify({"message": f"Email '{profile_id}' already exists in DefUser"}), 409)

        new_profile = DefAccessProfile(
            user_id          = user_id,
            profile_type     = profile_type,
            profile_id       = profile_id,
            primary_yn       = primary_yn,
            created_by       = get_jwt_identity(),
            creation_date    = datetime.utcnow(),
            last_updated_by  = get_jwt_identity(),
            last_update_date = datetime.utcnow()

        )

        db.session.add(new_profile)
        db.session.commit()
        return make_response(jsonify({"message": "Added successfully"}), 201)

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
@jwt_required()
def get_users_access_profiles():
    try:
        profiles = DefAccessProfile.query.all()
        return make_response(jsonify([profile.json() for profile in profiles]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Access Profiles", "error": str(e)}), 500)


@flask_app.route('/access_profiles/<int:user_id>', methods=['GET'])
@jwt_required()
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
@jwt_required()
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
        profile.last_updated_by = get_jwt_identity()
        profile.last_update_date = datetime.utcnow()

        # Commit changes to DefAccessProfile
        db.session.commit()

        return make_response(jsonify({"message": "Edited successfully"}), 200)

    except Exception as e:
        db.session.rollback()  # Rollback on error
        return make_response(jsonify({"message": "Error Editing Access Profile", "error": str(e)}), 500)


# Delete an access profile
@flask_app.route('/access_profiles/<int:user_id>/<int:serial_number>', methods=['DELETE'])
@jwt_required()
def delete_access_profile(user_id, serial_number):
    try:
        profile = DefAccessProfile.query.filter_by(user_id=user_id, serial_number=serial_number).first()
        if profile:
            db.session.delete(profile)
            db.session.commit()
            return make_response(jsonify({"message": "Deleted successfully"}), 200)
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
            execution_method = execution_method,
            internal_execution_method = internal_execution_method,
            executor = executor,
            description = description,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        # Add to session and commit
        db.session.add(new_method)
        db.session.commit()

        return jsonify({"message": "Added successfully", "data": new_method.json()}), 201

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
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200

    except Exception as e:
        return jsonify({"message": "Error searching execution methods", "error": str(e)}), 500



@flask_app.route('/Show_ExecutionMethod/<string:internal_execution_method>', methods=['GET'])
@jwt_required()
def Show_ExecutionMethod(internal_execution_method):
    try:
        method = db.session.query(DefAsyncExecutionMethods).filter_by(internal_execution_method=internal_execution_method).first()
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

            execution_method.last_updated_by = get_jwt_identity()

            # Update the last update timestamp
            execution_method.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({"message": "Edited successfully"}), 200)

        return make_response(jsonify({"message": f"Execution method with internal_execution_method '{internal_execution_method}' not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error editing execution method", "error": str(e)}), 500)


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

        return jsonify({"message": f"Deleted successfully"}), 200

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
            created_by = get_jwt_identity(),
            last_updated_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_update_date = datetime.utcnow()

        )
        db.session.add(new_task)
        db.session.commit()

        return {"message": "Added successfully"}, 201

    except Exception as e:
        return {"message": "Error creating Task", "error": str(e)}, 500


@flask_app.route('/def_async_tasks', methods=['GET'])
@jwt_required()
def Show_Tasks():
    try:
        tasks = DefAsyncTask.query.order_by(DefAsyncTask.def_task_id.desc()).all()
        return make_response(jsonify([task.json() for task in tasks]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting async Tasks", "error": str(e)}), 500)


@flask_app.route('/def_async_tasks/v1', methods=['GET'])
def Show_Tasks_v1():
    try:
        tasks = DefAsyncTask.query.order_by(DefAsyncTask.def_task_id.desc()).all()
        return make_response(jsonify([task.json() for task in tasks]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting async Tasks", "error": str(e)}), 500)


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
        return make_response(jsonify({"message": "Error getting async Tasks", "error": str(e)}), 500)


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
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
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
            task.last_updated_by = get_jwt_identity()
            task.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({"message": "Edited successfully"}), 200)

        return make_response(jsonify({"message": f"Async Task with name '{task_name}' not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error editing async Task", "error": str(e)}), 500)


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

            # return make_response(jsonify({"message": f"Task {task_name} has been cancelled successfully"}), 200)
            return make_response(jsonify({"message": "Cancelled successfully"}), 200)


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
            

            # Validate required fields
            if not (parameter_name and data_type):
                return jsonify({"error": "Missing required parameter fields"}), 400

            # Create a new parameter object
            new_param = DefAsyncTaskParam(
                task_name = task_name,
                parameter_name = parameter_name,
                data_type = data_type,
                description = description,
                created_by = get_jwt_identity(),
                creation_date = datetime.utcnow(),
                last_updated_by = get_jwt_identity(),
                last_update_date = datetime.utcnow()
            )
            new_params.append(new_param)

        # Add all new parameters to the session and commit
        db.session.add_all(new_params)
        db.session.commit()

        # return make_response(jsonify({
        #     "message": "Parameters Created successfully",
        #     "parameters": [param.json() for param in new_params]
        # }), 201)
        return make_response(jsonify({
            "message": "Added successfully",
        }), 201)
    except Exception as e:
        return jsonify({"error": "Failed to add task parameters", "details": str(e)}), 500



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
        param.last_updated_by = get_jwt_identity()
        param.last_update_date = datetime.utcnow()

        # Commit the changes to the database
        db.session.commit()

        # return jsonify({"message": "Task parameter updated successfully", 
        #                  "task_param": param.json()}), 200
        return jsonify({"message": "Edited successfully"}), 200

    except Exception as e:
        return jsonify({"error": "Error editing task parameter", "details": str(e)}), 500



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

        # return jsonify({"message": f"Parameter with def_param_id '{def_param_id}' successfully deleted from task '{task_name}'"}), 200
        return jsonify({"message": "Deleted successfully"}), 200


    except Exception as e:
        return jsonify({"error": "Failed to delete task parameter", "details": str(e)}), 500





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
                    user_schedule_name = user_schedule_name,
                    executor = executor,
                    task_name = task_name,
                    args = args,
                    kwargs = kwargs,
                    schedule_type = schedule_type,
                    cancelled_yn = 'N',
                    created_by = get_jwt_identity(),
                    creation_date = datetime.utcnow(),
                    last_updated_by = get_jwt_identity(),
                    last_update_date = datetime.utcnow()
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
            user_schedule_name = user_schedule_name,
            redbeat_schedule_name = redbeat_schedule_name,
            task_name = task_name,
            args = args,
            kwargs = kwargs,
            parameters = kwargs,
            schedule_type = schedule_type,
            schedule = schedule_data,
            cancelled_yn = 'N',
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(new_schedule)
        db.session.commit()

        # return jsonify({
        #     "message": "Task schedule created successfully!",
        #     "schedule_id": new_schedule.def_task_sche_id
        # }), 201
        return jsonify({"message": "Added successfully"}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": "Failed to add task schedule", "details": str(e)}), 500


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
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
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
        schedule.last_updated_by = get_jwt_identity()
        schedule.last_update_date = datetime.utcnow()

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
                schedule_name = redbeat_schedule_name,
                task = executors.executor,
                schedule_minutes = schedule_minutes,
                cron_schedule = cron_schedule,
                args = schedule.args,
                kwargs = schedule.kwargs,
                celery_app = celery
            )
        except Exception as e:
            db.session.rollback()
            return jsonify({"message": "Error updating Redis. Database changes rolled back.", "error": str(e)}), 500

        db.session.commit()
        # return jsonify({"message": f"Task Schedule for {redbeat_schedule_name} updated successfully in database and Redis"}), 200
        return jsonify({"message": "Edited successfully"}), 200


    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error editing Task Schedule", "error": str(e)}), 500


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
        # return make_response(jsonify({"message": f"Task periodic schedule for {redbeat_schedule_name} has been cancelled successfully in the database and deleted from Redis"}), 200)
        return make_response(jsonify({"message": "Cancelled successfully"}), 200)

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
            frequency_type_raw = schedule_data.get('FREQUENCY_TYPE', 'MINUTES')
            frequency_type = frequency_type_raw.upper().strip().rstrip('s').replace('(', '').replace(')', '')
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

        # return make_response(jsonify({'message': f"Schedule '{redbeat_schedule_name}' has been rescheduled."}), 200)
        return make_response(jsonify({'message': "Rescheduled Successfully."}), 200)


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
        fourteen_days = datetime.utcnow() - timedelta(days=2)
        tasks = DefAsyncTaskRequest.query.filter(DefAsyncTaskRequest.creation_date >= fourteen_days).order_by(DefAsyncTaskRequest.creation_date.desc())
        #tasks = DefAsyncTaskRequest.query.limit(100000).all()
        if not tasks:
            return jsonify({"message": "No tasks found"}), 404
        return jsonify([task.json() for task in tasks]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@flask_app.route('/view_requests_v2', methods=['GET'])
@jwt_required()
def view_requests_v2():
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
        # Query params
        days = request.args.get('days', type=int)
        search_query = request.args.get('task_name', '').strip().lower()

        query = DefAsyncTaskRequest.query

        # Case 1: task_name provided but days not provided -> default days = 30
        if search_query and days is None:
            days = 7

        # Apply days filter if available (now days will be set in all needed cases)
        if days is not None:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            query = query.filter(DefAsyncTaskRequest.creation_date >= cutoff_date)

        # Apply task_name search if provided (with TRIM to avoid space issues)
        if search_query:
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(or_(
                DefAsyncTaskRequest.task_name.ilike(f'%{search_query}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_space}%')
            ))


        # if search_query:
        #     normalized_search = search_query.replace('_', ' ')
        #     query = query.filter(or_(
        #         func.lower(func.replace(func.trim(DefAsyncTaskRequest.task_name), '_', ' ')).ilike(f'%{normalized_search}%')
        #     ))

        # Order newest first
        query = query.order_by(DefAsyncTaskRequest.creation_date.desc())

        # Paginate results
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
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error fetching view requests", "error": str(e)}), 500)



@flask_app.route('/view_requests_v3/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def combined_tasks_v3(page, limit):
    try:
        days = request.args.get('days', type=int)
        search_query = request.args.get('task_name', '').strip().lower()

        query = DefAsyncTaskRequest.query

        if search_query and days is None:
            days = 7

        if days is not None:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            query = query.filter(DefAsyncTaskRequest.creation_date >= cutoff_date)

        if search_query:
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(or_(
                DefAsyncTaskRequest.task_name.ilike(f'%{search_query}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_space}%')
            ))

        paginated = query.order_by(DefAsyncTaskRequest.creation_date.desc()) \
                         .paginate(page=page, per_page=limit, error_out=False)

        db_tasks = paginated.items

        flower_tasks = {}
        try:
            res = requests.get(f"{flower_url}/api/tasks", timeout=5)
            if res.status_code == 200:
                flower_tasks = res.json()
        except:
            pass  # keep flower_tasks empty if error

        items = []

        
        for t in db_tasks:
            item = t.json()  # get all DB fields
            # add Flower fields
            if t.task_id and t.task_id in flower_tasks:
                ftask = flower_tasks[t.task_id]
                item["uuid"] = ftask.get("uuid")
                item["state"] = ftask.get("state")
                item["worker"] = ftask.get("worker")
            else:
                item["uuid"] = None
                item["state"] = None
                item["worker"] = None
            items.append(item)

        return make_response(jsonify({
            "items": items,
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return jsonify({"error": str(e)}), 500




#def_access_models
@flask_app.route('/def_access_models', methods=['POST'])
@jwt_required()
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
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow(),
            revision = 0,
            revision_date = datetime.utcnow(),
            datasource_name = datasource_name  # FK assignment
        )
        db.session.add(new_def_access_model)
        db.session.commit()
        return make_response(jsonify({"message": "Added successfully"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)

@flask_app.route('/def_access_models', methods=['GET'])
@jwt_required()
def get_def_access_models():
    try:
        # Query parameters
        def_access_model_id = request.args.get('def_access_model_id', type=int)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)
        model_name = request.args.get('model_name', '').strip()

        # Case 1: Get Single Model by ID
        if def_access_model_id:
            model = DefAccessModel.query.filter_by(def_access_model_id=def_access_model_id).first()
            if model:
                return make_response(jsonify({"result": model.json()}), 200)
            else:
                return make_response(jsonify({"message": "Access Model not found"}), 404)

        # Base Query
        query = DefAccessModel.query

        # Case 2: Search by model_name
        if model_name:
            search_underscore = model_name.replace(' ', '_')
            search_space = model_name.replace('_', ' ')
            query = query.filter(
                or_(
                    DefAccessModel.model_name.ilike(f'%{model_name}%'),
                    DefAccessModel.model_name.ilike(f'%{search_underscore}%'),
                    DefAccessModel.model_name.ilike(f'%{search_space}%')
                )
            )

        # Order by ID descending
        query = query.order_by(DefAccessModel.def_access_model_id.desc())

        # Case 3: Pagination
        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [model.json() for model in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)

        # Case 4: Get All (No pagination)
        models = query.all()
        return make_response(jsonify({
            "result": [model.json() for model in models]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving access models",
            "error": str(e)
        }), 500)



@flask_app.route('/def_access_models', methods=['PUT'])
@jwt_required()
def update_def_access_model():
    try:
        def_access_model_id = request.args.get('def_access_model_id', type=int)
        if not def_access_model_id:
            return make_response(jsonify({'message': 'def_access_model_id query parameter is required'}), 400)
        model = DefAccessModel.query.filter_by(def_access_model_id=def_access_model_id).first()
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
            model.last_updated_by   = get_jwt_identity()
            model.last_update_date  = datetime.utcnow()
            model.revision          = model.revision + 1
            model.revision_date     = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Access Model not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error Editing Access Model', 'error': str(e)}), 500)

@flask_app.route('/def_access_models', methods=['DELETE'])
@jwt_required()
def delete_def_access_model():
    try:
        def_access_model_id = request.args.get('def_access_model_id', type=int)
        if not def_access_model_id:
            return make_response(jsonify({'message': 'def_access_model_id query parameter is required'}), 400)
        model = DefAccessModel.query.filter_by(def_access_model_id=def_access_model_id).first()
        if model:
            db.session.delete(model)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Access Model not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting Access Model', 'error': str(e)}), 500)


@flask_app.route('/def_access_models/cascade', methods=['DELETE'])
@jwt_required()
def cascade_delete_access_model():
    try:
        # Get the access model ID from query params
        def_access_model_id = request.args.get('def_access_model_id', type=int)
        if not def_access_model_id:
            return jsonify({'error': 'def_access_model_id is required'}), 400


        access_model_exists = db.session.query(
            db.exists().where(DefAccessModel.def_access_model_id == def_access_model_id)
        ).scalar()

        access_model_logic_exists = db.session.query(
            db.exists().where(DefAccessModelLogic.def_access_model_id == def_access_model_id)
        ).scalar()

        if not access_model_exists and not access_model_logic_exists:
            return jsonify({'error': f'No records found in def_access_models or def_access_model_logics for ID {def_access_model_id}'}), 404

        DefAccessModel.query.filter_by(def_access_model_id=def_access_model_id).delete(synchronize_session=False)

        # Delete all related logic records
        DefAccessModelLogic.query.filter_by(def_access_model_id=def_access_model_id).delete(synchronize_session=False)

        db.session.commit()

        return jsonify({'message': 'Deleted successfully'}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500





#def_access_model_logics
@flask_app.route('/def_access_model_logics', methods=['POST'])
@jwt_required()
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
            def_access_model_logic_id = def_access_model_logic_id,
            def_access_model_id = def_access_model_id,
            filter = filter_text,
            object = object_text,
            attribute = attribute,
            condition = condition,
            value = value,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )
        db.session.add(new_logic)
        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': f'Error: {str(e)}'}), 500)

@flask_app.route('/def_access_model_logics/upsert', methods=['POST'])
@jwt_required()
def upsert_def_access_model_logics():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a non-empty list of objects'}), 400)

        response = []
        created = False

        for data in data_list:
            def_access_model_logic_id = data.get('def_access_model_logic_id')
            model_id = data.get('def_access_model_id')
            filter_text = data.get('filter')
            object_text = data.get('object')
            attribute = data.get('attribute')
            condition  = data.get('condition')
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
                existing_logic.last_updated_by = get_jwt_identity()
                existing_logic.last_update_date = datetime.utcnow()


                db.session.add(existing_logic)

                # response.append({
                #     'def_access_model_logic_id': existing_logic.def_access_model_logic_id,
                #     'status': 'updated',
                #     'message': 'AccessModelLogic updated successfully'
                # })
                response.append({'message': 'Edited successfully'})

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
                    def_access_model_id = model_id,
                    filter = filter_text,
                    object = object_text,
                    attribute = attribute,
                    condition = condition,
                    value = value,
                    created_by = get_jwt_identity(),
                    creation_date = datetime.utcnow(),
                    last_updated_by = get_jwt_identity(),
                    last_update_date = datetime.utcnow()
                )
                db.session.add(new_logic)
                db.session.flush()

                # response.append({
                #     'def_access_model_logic_id': new_logic.def_access_model_logic_id,
                #     'status': 'created',
                #     'message': 'AccessModelLogic created successfully'
                # })
                response.append({'message': 'Added successfully'})
                created = True

        db.session.commit()

        status_code = 201 if created else 200
        return make_response(jsonify(response), status_code)

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
@jwt_required()
def get_def_access_model_logics():
    try:
        def_access_model_logic_id = request.args.get('def_access_model_logic_id', type=int)

        # Case 1: Get single record
        if def_access_model_logic_id:
            logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first()
            if logic:
                return make_response(jsonify({"result": logic.json()}), 200)
            else:
                return make_response(jsonify({'message': 'access model logic not found'}), 404)

        # Case 2: Get all
        logics = DefAccessModelLogic.query.order_by(
            DefAccessModelLogic.def_access_model_logic_id.desc()
        ).all()

        return make_response(jsonify({
            "result": [logic.json() for logic in logics]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            'message': 'Error retrieving access model logics',
            'error': str(e)
        }), 500)




@flask_app.route('/def_access_model_logics', methods=['PUT'])
@jwt_required()
def update_def_access_model_logic():
    try:
        def_access_model_logic_id = request.args.get('def_access_model_logic_id', type=int)
        if not def_access_model_logic_id:
            return make_response(jsonify({'message': 'def_access_model_logic_id query parameter is required'}), 400)
        logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first()
        if logic:
            # logic.def_access_model_id = request.json.get('def_access_model_id', logic.def_access_model_id)
            logic.filter = request.json.get('filter', logic.filter)
            logic.object = request.json.get('object', logic.object)
            logic.attribute = request.json.get('attribute', logic.attribute)
            logic.condition = request.json.get('condition', logic.condition)
            logic.value = request.json.get('value', logic.value)
            logic.last_updated_by = get_jwt_identity()
            logic.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Access Model Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing Access Model Logic', 'error': str(e)}), 500)


@flask_app.route('/def_access_model_logics', methods=['DELETE'])
@jwt_required()
def delete_def_access_model_logic():
    try:
        def_access_model_logic_id = request.args.get('def_access_model_logic_id', type=int)
        if not def_access_model_logic_id:
            return make_response(jsonify({'message': 'def_access_model_logic_id query parameter is required'}), 400)
        logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first()
        if logic:
            db.session.delete(logic)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Access Model Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting Access Model Logic', 'error': str(e)}), 500)





#def_access_model_logic_attributes
@flask_app.route('/def_access_model_logic_attributes', methods=['POST'])
@jwt_required()
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
            def_access_model_logic_id = def_access_model_logic_id,
            widget_position = widget_position,
            widget_state = widget_state,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )
        db.session.add(new_attribute)
        db.session.commit()
        return make_response(jsonify({"message": "Added successfully"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)


@flask_app.route('/def_access_model_logic_attributes', methods=['GET'])
@jwt_required()
def get_def_access_model_logic_attributes():
    try:
        id = request.args.get('id', type=int)

        # Case 1: Get single attribute by ID
        if id:
            attribute = DefAccessModelLogicAttribute.query.filter_by(id=id).first()
            if attribute:
                return make_response(jsonify({"result": attribute.json()}), 200)
            else:
                return make_response(jsonify({'message': 'Attribute not found'}), 404)

        # Case 2: Get all attributes
        attributes = DefAccessModelLogicAttribute.query.order_by(
            DefAccessModelLogicAttribute.id.desc()
        ).all()

        return make_response(jsonify({
            "result": [attr.json() for attr in attributes]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving attributes",
            "error": str(e)
        }), 500)


@flask_app.route('/def_access_model_logic_attributes/upsert', methods=['POST'])
@jwt_required()
def upsert_def_access_model_logic_attributes():
    try:
        data_list = request.get_json()

        # Enforce list-only payload
        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []
        created = False

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
                existing_attribute.last_updated_by = get_jwt_identity()
                existing_attribute.last_update_date = datetime.utcnow()

                db.session.add(existing_attribute)

                # response.append({
                #     'id': existing_attribute.id,
                #     'status': 'updated',
                #     'message': 'Attribute updated successfully'
                # })
                response.append({'message': 'Edited successfully'})

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
                    def_access_model_logic_id = def_access_model_logic_id,
                    widget_position = widget_position,
                    widget_state = widget_state,
                    created_by = get_jwt_identity(),
                    creation_date = datetime.utcnow(),
                    last_updated_by = get_jwt_identity(),
                    last_update_date = datetime.utcnow()
                )
                db.session.add(new_attribute)
                db.session.flush()

                # response.append({
                #     'id': new_attribute.id,
                #     'status': 'created',
                #     'message': 'Attribute created successfully'
                # })
                response.append({'message': 'Added successfully'})
                created =True


        db.session.commit()

        status_code = 201 if created else 200
        return make_response(jsonify(response), status_code)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error during upsert'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error during upsert',
            'error': str(e)
        }), 500)


@flask_app.route('/def_access_model_logic_attributes', methods=['PUT'])
@jwt_required()
def update_def_access_model_logic_attribute():
    try:
        id = request.args.get('id', type=int)
        if not id:
            return make_response(jsonify({'message': 'id query parameter is required'}), 400)
        attribute = DefAccessModelLogicAttribute.query.filter_by(id=id).first()
        if attribute:
            # attribute.def_access_model_logic_id = request.json.get('def_access_model_logic_id', attribute.def_access_model_logic_id)
            attribute.widget_position  = request.json.get('widget_position', attribute.widget_position)
            attribute.widget_state     = request.json.get('widget_state', attribute.widget_state)
            attribute.last_updated_by  = get_jwt_identity()
            attribute.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Attribute not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing attribute', 'error': str(e)}), 500)


@flask_app.route('/def_access_model_logic_attributes', methods=['DELETE'])
@jwt_required()
def delete_def_access_model_logic_attribute():
    try:
        id = request.args.get('id', type=int)
        if not id:
            return make_response(jsonify({'message': 'id query parameter is required'}), 400)
        attribute = DefAccessModelLogicAttribute.query.filter_by(id=id).first()
        if attribute:
            db.session.delete(attribute)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Attribute not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting attribute', 'error': str(e)}), 500)





# def_global_conditions
@flask_app.route('/def_global_conditions', methods=['POST'])
@jwt_required()
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
            status      = status,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(new_condition)
        db.session.commit()

        return make_response(jsonify({"message": "Added successfully"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)

@flask_app.route('/def_global_conditions', methods=['GET'])
@jwt_required()
def get_def_global_conditions():
    try:
        # Query parameters
        def_global_condition_id = request.args.get('def_global_condition_id', type=int)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)
        name = request.args.get('name', '').strip()

        # Case 1: Get Single Condition by ID
        if def_global_condition_id:
            condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
            if condition:
                return make_response(jsonify({"result": condition.json()}), 200)
            return make_response(jsonify({"message": "Global condition not found"}), 404)

        # Base Query
        query = DefGlobalCondition.query

        # Case 2: Search by name
        if name:
            search_underscore = name.replace(' ', '_')
            search_space = name.replace('_', ' ')
            query = query.filter(
                or_(
                    DefGlobalCondition.name.ilike(f'%{name}%'),
                    DefGlobalCondition.name.ilike(f'%{search_underscore}%'),
                    DefGlobalCondition.name.ilike(f'%{search_space}%')
                )
            )

        # Order by ID descending
        query = query.order_by(DefGlobalCondition.def_global_condition_id.desc())

        # Case 3: Pagination
        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [item.json() for item in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)

        # Case 4: Get All (No pagination, no ID)
        conditions = query.all()
        return make_response(jsonify({
            "result": [condition.json() for condition in conditions]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving GlobalConditions",
            "error": str(e)
        }), 500)


@flask_app.route('/def_global_conditions', methods=['PUT'])
@jwt_required()
def update_def_global_condition():
    try:
        def_global_condition_id = request.args.get('def_global_condition_id', type=int)
        if not def_global_condition_id:
            return make_response(jsonify({'message': 'def_global_condition_id query parameter is required'}), 400)
        condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
        if condition:
            condition.name        = request.json.get('name', condition.name)
            condition.datasource  = request.json.get('datasource', condition.datasource)
            condition.description = request.json.get('description', condition.description)
            condition.status      = request.json.get('status', condition.status)
            condition.last_updated_by = get_jwt_identity()
            condition.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Global Condition not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing Global Condition', 'error': str(e)}), 500)

@flask_app.route('/def_global_conditions', methods=['DELETE'])
@jwt_required()
def delete_def_global_condition():
    try:
        def_global_condition_id = request.args.get('def_global_condition_id', type=int)
        if not def_global_condition_id:
            return make_response(jsonify({'message': 'def_global_condition_id query parameter is required'}), 400)
        condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
        if condition:
            db.session.delete(condition)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Global Condition not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting Global Condition', 'error': str(e)}), 500)



@flask_app.route('/def_global_conditions/cascade', methods=['DELETE'])
@jwt_required()
def cascade_delete_global_condition():
    try:
        # Get condition ID from query parameter
        def_global_condition_id = request.args.get('def_global_condition_id', type=int)
        if not def_global_condition_id:
            return jsonify({'error': 'def_global_condition_id is required'}), 400
        
        global_condition_exists = db.session.query(
            db.exists().where(DefGlobalCondition.def_global_condition_id == def_global_condition_id)
        ).scalar()

        global_condition_logic_exists = db.session.query(
            db.exists().where(DefGlobalConditionLogic.def_global_condition_id == def_global_condition_id)
        ).scalar()

        if not global_condition_exists and not global_condition_logic_exists:
            return jsonify({'error': f'No records found in def_global_conditions or def_global_condition_logics for ID {def_global_condition_id}'}), 404

        DefGlobalConditionLogic.query.filter_by(def_global_condition_id=def_global_condition_id).delete(synchronize_session=False)

        DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).delete(synchronize_session=False)

        db.session.commit()

        return jsonify({'message': 'Deleted successfully'}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500



# def_global_condition_logics
@flask_app.route('/def_global_condition_logics', methods=['POST'])
@jwt_required()
def create_def_global_condition_logic():
    try:
        def_global_condition_logic_id = request.json.get('def_global_condition_logic_id')

        if def_global_condition_logic_id is None:
            return make_response(jsonify({"message": "Missing 'def_global_condition_logic_id'"}), 400)

        # Check if ID already exists
        existing = db.session.get(DefGlobalConditionLogic, def_global_condition_logic_id)
        if existing:
            return make_response(jsonify({
                "message": f"Global Condition Logic ID {def_global_condition_logic_id} already exists."
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
            value = value,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )
        db.session.add(new_logic)
        db.session.commit()
        # return make_response(jsonify({'def_global_condition_logic_id' : new_logic.def_global_condition_logic_id,
        #                               'message': 'Global Condition Logic created successfully'}), 201)
        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)

@flask_app.route('/def_global_condition_logics/upsert', methods=['POST'])
@jwt_required()
def upsert_def_global_condition_logics():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []
        created = False

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
                existing_logic.last_updated_by = get_jwt_identity()
                existing_logic.last_update_date = datetime.utcnow()


                db.session.add(existing_logic)

                # response.append({
                #     'def_global_condition_logic_id': existing_logic.def_global_condition_logic_id,
                #     'status': 'updated',
                #     'message': 'Logic updated successfully'
                # })
                response.append({'message': 'Edited successfully'})

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
                    def_global_condition_logic_id = def_global_condition_logic_id,
                    def_global_condition_id = def_global_condition_id,
                    object = object_text,
                    attribute = attribute,
                    condition = condition,
                    value = value,
                    created_by = get_jwt_identity(),
                    creation_date = datetime.utcnow(),
                    last_updated_by = get_jwt_identity(),
                    last_update_date = datetime.utcnow()
                )
                db.session.add(new_logic)
                db.session.flush()

                # response.append({
                #     'def_global_condition_logic_id': new_logic.def_global_condition_logic_id,
                #     'status': 'created',
                #     'message': 'Logic created successfully'
                # })
                response.append({'message': 'Added successfully'})
                created = True

        db.session.commit()
        status_code = 201 if created else 200
        return make_response(jsonify(response), status_code)

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
@jwt_required()
def get_def_global_condition_logics():
    try:
        def_global_condition_logic_id = request.args.get('def_global_condition_logic_id', type=int)

        # Case 1: Single logic by ID
        if def_global_condition_logic_id:
            logic = DefGlobalConditionLogic.query.filter_by(
                def_global_condition_logic_id=def_global_condition_logic_id
            ).first()

            if logic:
                return make_response(jsonify({"result": logic.json()}), 200)

            return make_response(jsonify({
                "message": "Global Condition Logic not found"
            }), 404)

        # Case 2: Get all logics
        logics = DefGlobalConditionLogic.query.order_by(
            DefGlobalConditionLogic.def_global_condition_logic_id.desc()
        ).all()

        return make_response(jsonify({
            "result": [logic.json() for logic in logics]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving Global Condition Logics",
            "error": str(e)
        }), 500)



@flask_app.route('/def_global_condition_logics', methods=['PUT'])
@jwt_required()
def update_def_global_condition_logic():
    try:
        def_global_condition_logic_id = request.args.get('def_global_condition_logic_id', type=int)
        if not def_global_condition_logic_id:
            return make_response(jsonify({'message': 'def_global_condition_logic_id query parameter is required'}), 400)
        logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()
        if logic:
            logic.def_global_condition_id = request.json.get('def_global_condition_id', logic.def_global_condition_id)
            logic.object = request.json.get('object', logic.object)
            logic.attribute = request.json.get('attribute', logic.attribute)
            logic.condition = request.json.get('condition', logic.condition)
            logic.value = request.json.get('value', logic.value)
            logic.last_updated_by = get_jwt_identity()
            logic.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Global Condition Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing Global Condition Logic', 'error': str(e)}), 500)


@flask_app.route('/def_global_condition_logics', methods=['DELETE'])
@jwt_required()
def delete_def_global_condition_logic():
    try:
        def_global_condition_logic_id = request.args.get('def_global_condition_logic_id', type=int)
        if not def_global_condition_logic_id:
            return make_response(jsonify({'message': 'def_global_condition_logic_id query parameter is required'}), 400)
        logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()
        if logic:
            db.session.delete(logic)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Global Condition Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting Global Condition Logic', 'error': str(e)}), 500)





# def_global_condition_logics_attributes
@flask_app.route('/def_global_condition_logic_attributes', methods=['POST'])
@jwt_required()
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
            id = id,
            def_global_condition_logic_id = def_global_condition_logic_id,
            widget_position = widget_position,
            widget_state = widget_state,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(new_attr)
        db.session.commit()

        # return make_response(jsonify({
        #     'id': new_attr.id,
        #     'message': 'Attribute created successfully'
        # }), 201)
        return make_response(jsonify({'message': 'Added successfully'}), 201)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error (possibly duplicate key)'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error creating attribute', 'error': str(e)}), 500)



@flask_app.route('/def_global_condition_logic_attributes', methods=['GET'])
@jwt_required()
def get_def_global_condition_logic_attributes():
    try:
        id = request.args.get('id', type=int)

        # Case 1: Get single attribute by ID
        if id:
            attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()
            if attribute:
                return make_response(jsonify({"result": attribute.json()}), 200)
            return make_response(jsonify({
                "message": "Global Condition Logic Attribute not found"
            }), 404)

        # Case 2: Get all attributes
        attributes = DefGlobalConditionLogicAttribute.query.order_by(
            DefGlobalConditionLogicAttribute.id.desc()
        ).all()

        return make_response(jsonify({
            "result": [attribute.json() for attribute in attributes]
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving condition logic attributes",
            "error": str(e)
        }), 500)
   



@flask_app.route('/def_global_condition_logic_attributes/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
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
@jwt_required()
def upsert_def_global_condition_logic_attributes():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []
        created = False

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
                existing_attr.last_updated_by = get_jwt_identity()
                existing_attr.last_update_date = datetime.utcnow()

                db.session.add(existing_attr)

                # response.append({
                #     'id': existing_attr.id,
                #     'status': 'updated',
                #     'message': 'Attribute updated successfully'
                # })
                response.append({'message': 'Edited successfully'})

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
                    def_global_condition_logic_id = def_global_condition_logic_id,
                    widget_position = widget_position,
                    widget_state = widget_state,
                    created_by = get_jwt_identity(),
                    creation_date = datetime.utcnow(),
                    last_updated_by = get_jwt_identity(),
                    last_update_date = datetime.utcnow()
                )
                db.session.add(new_attr)
                db.session.flush()

                # response.append({
                #     'id': new_attr.id,
                #     'status': 'created',
                #     'message': 'Attribute created successfully'
                # })
                response.append({'message': 'Added successfully'})
                created = True

        db.session.commit()
        status_code = 201 if created else 200
        return make_response(jsonify(response), status_code)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error during upsert'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error during upsert',
            'error': str(e)
        }), 500)


@flask_app.route('/def_global_condition_logic_attributes', methods=['PUT'])
@jwt_required()
def update_def_global_condition_logic_attribute():
    try:
        id = request.args.get('id', type=int)
        if not id:
            return make_response(jsonify({'message': 'id query parameter is required'}), 400)
        data = request.get_json()
        attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()

        if not attribute:
            return make_response(jsonify({'message': 'Global Condition Logic Attribute not found'}), 404)

        # Update allowed fields
        attribute.widget_position = data.get('widget_position', attribute.widget_position)
        attribute.widget_state = data.get('widget_state', attribute.widget_state)
        attribute.last_updated_by = get_jwt_identity()
        attribute.last_update_date = datetime.utcnow()

        db.session.commit()

        return make_response(jsonify({'message': 'Edited successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error editing Global Condition Logic Attribute',
            'error': str(e)
        }), 500)




@flask_app.route('/def_global_condition_logic_attributes', methods=['DELETE'])
@jwt_required()
def delete_def_global_condition_logic_attribute():
    try:
        id = request.args.get('id', type=int)
        if not id:
            return make_response(jsonify({'message': 'id query parameter is required'}), 400)
        attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()

        if not attribute:
            return make_response(jsonify({'message': 'Global Condition Logic Attribute not found'}), 404)

        db.session.delete(attribute)
        db.session.commit()

        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error deleting Global Condition Logic Attribute',
            'error': str(e)
        }), 500)











#def_access_points

@flask_app.route("/def_access_points", methods=["POST"])
@jwt_required()
def create_access_point():
    try:
        data = request.get_json() or {}

        access_point_name = data.get("access_point_name")
        description = data.get("description")
        platform = data.get("platform")
        access_point_type = data.get("access_point_type")
        access_control = data.get("access_control")
        change_control = data.get("change_control")
        audit = data.get("audit")
        def_data_source_id = data.get("def_data_source_id")
        def_entitlement_id = data.get("def_entitlement_id")


        data_source = db.session.query(DefDataSource).filter_by(def_data_source_id=def_data_source_id).first()
        if not data_source:
            return make_response(jsonify({
                "message": "Invalid data source ID",
                "error": "Data source not found"
            }), 400)

        access_point = DefAccessPoint(
            def_data_source_id = def_data_source_id,
            access_point_name = access_point_name,
            description = description,
            platform = platform,
            access_point_type = access_point_type,
            access_control = access_control,
            change_control = change_control,
            audit = audit,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(access_point)
        db.session.flush()

        entitlement_element = DefAccessEntitlementElement(
            def_entitlement_id = def_entitlement_id,
            def_access_point_id = access_point.def_access_point_id,
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        
        db.session.add(entitlement_element)
        db.session.commit()

        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating access point', 'error': str(e)}), 500)



@flask_app.route("/def_access_points", methods=["GET"])
@jwt_required()
def get_access_points():
    try:
        def_access_point_id = request.args.get("def_access_point_id", type=int)
        access_point_name = request.args.get("access_point_name", type=str)
        page = request.args.get("page", type=int)
        limit = request.args.get("limit", type=int)

        query = DefAccessPoint.query

        # Filter by access_point_name if provided
        if access_point_name:
            query = query.filter(DefAccessPoint.access_point_name.ilike(f"%{access_point_name}%"))

        # Fetch single access point
        if def_access_point_id:
            access_point = query.filter_by(def_access_point_id=def_access_point_id).first()
            if not access_point:
                return make_response(jsonify({
                    "message": "Access point not found"
                }), 404)
            return jsonify(access_point.json())

        if page and limit:
            pagination = query.order_by(DefAccessPoint.creation_date.desc()).paginate(
                page=page, per_page=limit, error_out=False
            )
            access_points = pagination.items
            results = [ap.json() for ap in access_points]
            return jsonify({
                "items": results,
                "page": pagination.page,
                "pages": pagination.pages,
                "total": pagination.total
            })
        else:
            access_points = query.order_by(DefAccessPoint.creation_date.desc()).all()
            results = [ap.json() for ap in access_points]
            return jsonify(results)

    except Exception as e:
        return make_response(jsonify({"message": "Error fetching access points", "error": str(e)}), 500)


@flask_app.route("/def_access_points_view", methods=["GET"])
@jwt_required()
def get_access_point_view():
    try:
        def_access_point_id = request.args.get("def_access_point_id", type=int)
        def_entitlement_id = request.args.get("def_entitlement_id", type=int)
        access_point_name = request.args.get("access_point_name", type=str)
        unlinked = request.args.get("unlinked", type=str)
        page = request.args.get("page", type=int)
        limit = request.args.get("limit", type=int)

        query = DefAccessPointsV.query

        
        if access_point_name:
            query = query.filter(DefAccessPointsV.access_point_name.ilike(f"%{access_point_name}%"))

        if unlinked and unlinked.lower() == "true":
            query = query.filter(DefAccessPointsV.def_entitlement_id.is_(None))

        # Fetch single access point
        if def_access_point_id:
            access_point = query.filter_by(def_access_point_id=def_access_point_id).first()
            if not access_point:
                return make_response(jsonify({
                    "message": "Access point not found"
                }), 404)
            return jsonify(access_point.json())


        if def_entitlement_id:
            entitlement = query.filter_by(def_entitlement_id=def_entitlement_id).all()
            if not entitlement:
                return make_response(jsonify({
                    "message":  "No access point found"
                }), 404)
            results = [ent.json() for ent in entitlement]
            return jsonify(results)

        if page and limit:
            pagination = query.order_by(DefAccessPointsV.creation_date.desc()).paginate(
                page=page, per_page=limit, error_out=False
            )
            access_points = pagination.items
            results = [ap.json() for ap in access_points]
            return jsonify({
                "items": results,
                "page": pagination.page,
                "pages": pagination.pages,
                "total": pagination.total
            })
        else:
            access_points = query.order_by(DefAccessPointsV.creation_date.desc()).all()
            results = [ap.json() for ap in access_points]
            return jsonify(results)

    except Exception as e:
        return make_response(jsonify({"message": "Error fetching access points", "error": str(e)}), 500)
    


@flask_app.route("/def_access_points", methods=["PUT"])
@jwt_required()
def update_access_point():
    try:
        def_access_point_id = request.args.get("def_access_point_id", type=int)
        if not def_access_point_id:
            return make_response(jsonify({"message": "def_access_point_id is required"}), 400)

        data = request.get_json() or {}
        user_id = get_jwt_identity()

        ap = db.session.query(DefAccessPoint).filter_by(def_access_point_id=def_access_point_id).first()
        if not ap:
            return make_response(jsonify({
                "message": "Access point not found"
            }), 404)


        def_data_source_id = data.get("def_data_source_id")
        if def_data_source_id is not None:
            data_source = db.session.query(DefDataSource).filter_by(def_data_source_id=def_data_source_id).first()
            if not data_source:
                return make_response(jsonify({
                    "message": "Invalid data source ID",
                    "error": "Data source not found"
                }), 400)

        def_entitlement_id = data.get("def_entitlement_id")


        ap.access_point_name = data.get("access_point_name", ap.access_point_name)
        ap.description = data.get("description", ap.description)
        ap.platform = data.get("platform", ap.platform)
        ap.access_point_type = data.get("access_point_type", ap.access_point_type)
        ap.access_control = data.get("access_control", ap.access_control)
        ap.change_control = data.get("change_control", ap.change_control)
        ap.audit = data.get("audit", ap.audit)
        ap.def_data_source_id = def_data_source_id if def_data_source_id is not None else ap.def_data_source_id
        ap.last_updated_by = user_id
        ap.last_update_date = datetime.utcnow()

        if def_entitlement_id is not None:
            entitlement_element = DefAccessEntitlementElement.query.filter_by(
                def_access_point_id=def_access_point_id
            ).first()  

            if entitlement_element:
                entitlement_element.def_entitlement_id = def_entitlement_id
                entitlement_element.last_updated_by = user_id
                entitlement_element.last_update_date = datetime.utcnow()



        db.session.commit()

        return make_response(jsonify({"message": "Edited successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error editing access point","error": str(e)}), 500)




@flask_app.route("/def_access_points", methods=["DELETE"])
@jwt_required()
def delete_access_point():
    try:
        # Get the access point ID from query params
        def_access_point_id = request.args.get("def_access_point_id", type=int)
        if not def_access_point_id:
            return make_response(jsonify({"message": "def_access_point_id is required"}), 400)

        # Fetch the access point
        access_point = db.session.get(DefAccessPoint, def_access_point_id)
        if not access_point:
            return make_response(jsonify({
                "message": f"Access point with id {def_access_point_id} not found"
            }), 404)

        # Delete the access point
        db.session.delete(access_point)
        db.session.commit()

        return jsonify({"message": "Deleted successfully"})

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "message": "Error deleting access point",
            "error": str(e)
        }), 500)





#Def_Data_Sources
@flask_app.route('/def_data_sources', methods=['POST'])
@jwt_required()
def create_def_data_source():
    try:
        new_datasource = DefDataSource(
            datasource_name=request.json.get('datasource_name'),
            description=request.json.get('description'),
            application_type=request.json.get('application_type'),
            application_type_version=request.json.get('application_type_version'),
            last_access_synchronization_date=request.json.get('last_access_synchronization_date'),
            last_access_synchronization_status=request.json.get('last_access_synchronization_status'),
            last_transaction_synchronization_date=request.json.get('last_transaction_synchronization_date'),
            last_transaction_synchronization_status=request.json.get('last_transaction_synchronization_status'),
            default_datasource=request.json.get('default_datasource'),
            created_by=get_jwt_identity(),
            last_updated_by=get_jwt_identity(),
            creation_date=datetime.utcnow(),
            last_update_date=datetime.utcnow()
        )
        db.session.add(new_datasource)
        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating data source', 'error': str(e)}), 500)


@flask_app.route('/def_data_sources', methods=['GET'])
@jwt_required()
def get_def_data_sources():
    try:
        def_data_source_id = request.args.get('def_data_source_id', type=int)
        datasource_name = request.args.get('datasource_name', type=str)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        # Case 1: Get by ID
        if def_data_source_id:
            ds = DefDataSource.query.filter_by(def_data_source_id=def_data_source_id).first()
            if ds:
                return make_response(jsonify({'result': ds.json()}), 200)
            return make_response(jsonify({'message': 'Data source not found'}), 404)

        query = DefDataSource.query

        # Case 2: Search
        if datasource_name:
            search_query = datasource_name.strip()
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(
                or_(
                    DefDataSource.datasource_name.ilike(f'%{search_query}%'),
                    DefDataSource.datasource_name.ilike(f'%{search_underscore}%'),
                    DefDataSource.datasource_name.ilike(f'%{search_space}%')
                )
            )

        # Case 3: Pagination (Search or just List)
        if page and limit:
            paginated = query.order_by(DefDataSource.def_data_source_id.desc()).paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [ds.json() for ds in paginated.items],
                "total": paginated.total,
                "pages": 1 if paginated.total == 0 else paginated.pages,
                "page": paginated.page
            }), 200)

        # Case 4: Get All (if no ID and no pagination)
        data_sources = query.order_by(DefDataSource.def_data_source_id.desc()).all()
        return make_response(jsonify({'result': [ds.json() for ds in data_sources]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching data sources', 'error': str(e)}), 500)

@flask_app.route('/def_data_sources', methods=['PUT'])
@jwt_required()
def update_def_data_source():
    try:
        def_data_source_id = request.args.get('def_data_source_id', type=int)
        if not def_data_source_id:
            return make_response(jsonify({'message': 'def_data_source_id is required'}), 400)

        ds = DefDataSource.query.filter_by(def_data_source_id=def_data_source_id).first()
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
            ds.last_updated_by = get_jwt_identity()
            ds.last_update_date = datetime.utcnow()
            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Data source not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing data source', 'error': str(e)}), 500)


@flask_app.route('/def_data_sources', methods=['DELETE'])
@jwt_required()
def delete_def_data_source():
    try:
        def_data_source_id = request.args.get('def_data_source_id', type=int)
        if not def_data_source_id:
            return make_response(jsonify({'message': 'def_data_source_id is required'}), 400)

        ds = DefDataSource.query.filter_by(def_data_source_id=def_data_source_id).first()
        if ds:
            db.session.delete(ds)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Data source not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting data source', 'error': str(e)}), 500)











#def_access_entitlements
@flask_app.route('/def_access_entitlements', methods=['GET'])
@jwt_required()
def get_access_entitlements():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        entitlement_name = request.args.get('entitlement_name', type=str)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        # Case 1: Get by ID
        if def_entitlement_id:
            entitlement = DefAccessEntitlement.query.filter_by(def_entitlement_id=def_entitlement_id).first()
            if entitlement:
                return make_response(jsonify({'result': entitlement.json()}), 200)
            return make_response(jsonify({'message': 'Entitlement not found'}), 404)

        query = DefAccessEntitlement.query

        # Case 2: Search
        if entitlement_name:
            search_query = entitlement_name.strip()
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(
                or_(
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_query}%'),
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_underscore}%'),
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_space}%')
                )
            )

        # Case 3: Pagination (Search or just List)
        if page and limit:
            paginated = query.order_by(DefAccessEntitlement.def_entitlement_id.desc()).paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [e.json() for e in paginated.items],
                "total": paginated.total,
                "pages": 1 if paginated.total == 0 else paginated.pages,
                "page": paginated.page
            }), 200)

        # Case 4: Get All (if no ID and no pagination)
        entitlements = query.order_by(DefAccessEntitlement.def_entitlement_id.desc()).all()
        return make_response(jsonify({'result': [e.json() for e in entitlements]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching entitlements', 'error': str(e)}), 500)


@flask_app.route('/def_access_entitlements/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
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
        return make_response(jsonify({'message': 'Error fetching entitlements', 'error': str(e)}), 500)


@flask_app.route('/def_access_entitlements', methods=['POST'])
@jwt_required()
def create_entitlement():
    try:
        new_e = DefAccessEntitlement(
            entitlement_name = request.json.get('entitlement_name'),
            description = request.json.get('description'),
            comments = request.json.get('comments'),
            status = request.json.get('status'),
            effective_date = datetime.utcnow(),
            revision = 0,
            revision_date = datetime.utcnow(),
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )
        db.session.add(new_e)
        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating entitlement', 'error': str(e)}), 500)


@flask_app.route('/def_access_entitlements', methods=['PUT'])
@jwt_required()
def update_entitlement():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        if not def_entitlement_id:
            return make_response(jsonify({'message': 'def_entitlement_id is required'}), 400)

        entitlement = DefAccessEntitlement.query.filter_by(def_entitlement_id=def_entitlement_id).first()
        if entitlement:
            entitlement.entitlement_name = request.json.get('entitlement_name', entitlement.entitlement_name)
            entitlement.description = request.json.get('description', entitlement.description)
            entitlement.comments = request.json.get('comments', entitlement.comments)
            entitlement.status = request.json.get('status', entitlement.status)
            entitlement.effective_date = datetime.utcnow()
            entitlement.revision =  entitlement.revision + 1
            entitlement.revision_date = datetime.utcnow()
            entitlement.last_updated_by = get_jwt_identity()
            entitlement.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Entitlement not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing entitlement', 'error': str(e)}), 500)


@flask_app.route('/def_access_entitlements', methods=['DELETE'])
@jwt_required()
def delete_entitlement():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        if not def_entitlement_id:
            return make_response(jsonify({'message': 'def_entitlement_id is required'}), 400)

        entitlement = DefAccessEntitlement.query.filter_by(def_entitlement_id=def_entitlement_id).first()
        if entitlement:
            db.session.delete(entitlement)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Entitlement not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting entitlement', 'error': str(e)}), 500)


@flask_app.route('/def_access_entitlements/cascade', methods=['DELETE'])
@jwt_required()
def cascade_delete_entitlement():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        if not def_entitlement_id:
            return jsonify({'message': 'def_entitlement_id is required'}), 400

        entitlement_exists = db.session.query(db.exists().where(DefAccessEntitlement.def_entitlement_id == def_entitlement_id)).scalar()

        entitlement_elements_exists = db.session.query(db.exists().where(DefAccessEntitlementElement.def_access_entitlement_id == def_entitlement_id)).scalar()

        if not entitlement_exists and not entitlement_elements_exists:
            return jsonify({'error': f'No records found in def_access_entitlements or def_access_entitlement_elements for ID {def_entitlement_id}'}), 404

        DefAccessEntitlement.query.filter_by(def_entitlement_id=def_entitlement_id).delete(synchronize_session=False)

        DefAccessEntitlementElement.query.filter_by(def_entitlement_id=def_entitlement_id).delete(synchronize_session=False)


        db.session.commit()

        return jsonify({'message': 'Deleted successfully'}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500




#Def_access_entitlement_elements

@flask_app.route('/def_access_entitlement_elements', methods=['POST'])
@jwt_required()
def create_entitlement_element():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        if not def_entitlement_id:
            return make_response(jsonify({'error': 'def_entitlement_id is required'}), 400)

        data = request.get_json()
        user_id = get_jwt_identity()

        # Validate entitlement
        entitlement = db.session.get(DefAccessEntitlement, def_entitlement_id)
        if not entitlement:
            return make_response(jsonify({'error': 'Invalid def_entitlement_id'}), 400)

        # Expect a list of access point IDs
        access_point_ids = data.get('def_access_point_ids') or [data.get('def_access_point_id')]
        if not access_point_ids or not isinstance(access_point_ids, list):
            return make_response(jsonify({'error': 'def_access_point_ids must be a list'}), 400)

        for access_point_id in access_point_ids:
            # Validate access point
            access_point = db.session.get(DefAccessPoint, access_point_id)
            if not access_point:
                return make_response(jsonify({'error': 'Invalid access point ID'}), 400)

            # Skip duplicates
            exists = (
                db.session.query(DefAccessEntitlementElement)
                .filter_by(def_entitlement_id=def_entitlement_id, def_access_point_id=access_point_id)
                .first()
            )
            if exists:
                continue

            # Add entitlement element
            db.session.add(DefAccessEntitlementElement(
                def_entitlement_id = def_entitlement_id,
                def_access_point_id = access_point_id,
                created_by = user_id,
                last_updated_by = user_id,
                creation_date = datetime.utcnow(),
                last_update_date = datetime.utcnow()
            ))


        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully'}), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'error': str(e)}), 400)





@flask_app.route('/def_access_entitlement_elements', methods=['GET'])
@jwt_required()
def get_entitlement_elements():
    try:
        # Query parameters
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        # def_access_point_id = request.args.get('def_access_point_id', type=int)

        query = DefAccessEntitlementElement.query

        if def_entitlement_id:
            query = query.filter(DefAccessEntitlementElement.def_entitlement_id == def_entitlement_id)
        # if def_access_point_id:
        #     query = query.filter(DefAccessEntitlementElement.def_access_point_id == def_access_point_id)

        # Return all filtered records (or all if no filter)
        elements = query.order_by(DefAccessEntitlementElement.creation_date.desc()).all()
        return make_response(jsonify([e.json() for e in elements]), 200)

    except Exception as e:
        return make_response(jsonify({'error': str(e)}), 400)



@flask_app.route('/def_access_entitlement_elements', methods=['DELETE'])
@jwt_required()
def delete_entitlement_element():
    try:
        def_entitlement_id = request.args.get('def_entitlement_id', type=int)
        if not def_entitlement_id:
            return make_response(jsonify({'message': 'def_entitlement_id is required'}), 400)

        data = request.get_json()
        access_point_ids = data.get('def_access_point_ids')

        if not access_point_ids or not isinstance(access_point_ids, list):
            return make_response(jsonify({'message': 'def_access_point_ids (list) is required'}), 400)

        # Validate entitlement
        entitlement = db.session.get(DefAccessEntitlement, def_entitlement_id)
        if not entitlement:
            return make_response(jsonify({'error': 'Invalid def_entitlement_id'}), 400)

        db.session.query(DefAccessEntitlementElement).filter(
            DefAccessEntitlementElement.def_entitlement_id == def_entitlement_id,
            DefAccessEntitlementElement.def_access_point_id.in_(access_point_ids)
        ).delete(synchronize_session=False)

        db.session.commit()
        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'error': str(e)}), 400)






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
def get_def_controls():
    try:
        def_control_id = request.args.get('def_control_id', type=int)
        control_name = request.args.get('control_name', type=str)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        # Case 1: Get by ID
        if def_control_id:
            control = DefControl.query.filter_by(def_control_id=def_control_id).first()
            if control:
                return make_response(jsonify({'result': control.json()}), 200)
            return make_response(jsonify({'message': 'Control not found'}), 404)

        query = DefControl.query

        # Case 2: Search
        if control_name:
            search_query = control_name.strip()
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(
                or_(
                    DefControl.control_name.ilike(f'%{search_query}%'),
                    DefControl.control_name.ilike(f'%{search_underscore}%'),
                    DefControl.control_name.ilike(f'%{search_space}%')
                )
            )

        # Case 3: Pagination (Search or just List)
        if page and limit:
            paginated = query.order_by(DefControl.def_control_id.desc()).paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [control.json() for control in paginated.items],
                "total": paginated.total,
                "pages": 1 if paginated.total == 0 else paginated.pages,
                "page": paginated.page
            }), 200)

        # Case 4: Get All (if no ID and no pagination)
        controls = query.order_by(DefControl.def_control_id.desc()).all()
        return make_response(jsonify({'result': [c.json() for c in controls]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching controls', 'error': str(e)}), 500)


@flask_app.route('/def_controls', methods=['POST'])
@jwt_required()
def create_control():
    try:
        new_control = DefControl(
            control_name = request.json.get('control_name'),
            description = request.json.get('description'),
            pending_results_count = request.json.get('pending_results_count'),
            control_type = request.json.get('control_type'),
            priority = request.json.get('priority'),
            datasources = request.json.get('datasources'),
            last_run_date = datetime.utcnow(),
            status = request.json.get('status'),
            state = request.json.get('state'),
            result_investigator = request.json.get('result_investigator'),
            authorized_data = request.json.get('authorized_data'),
            revision = 0,
            revision_date = datetime.utcnow(),
            created_by = get_jwt_identity(),
            creation_date = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )
        db.session.add(new_control)
        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error adding control', 'error': str(e)}), 500)



@flask_app.route('/def_controls', methods=['PUT'])
@jwt_required()
def update_control():
    try:
        def_control_id = request.args.get('def_control_id', type=int)
        if not def_control_id:
            return make_response(jsonify({'message': 'def_control_id is required'}), 400)

        control = DefControl.query.filter_by(def_control_id=def_control_id).first()
        if control:
            control.control_name = request.json.get('control_name', control.control_name)
            control.description = request.json.get('description', control.description)
            control.pending_results_count = request.json.get('pending_results_count', control.pending_results_count)
            control.control_type = request.json.get('control_type', control.control_type)
            control.priority = request.json.get('priority', control.priority)
            control.datasources = request.json.get('datasources', control.datasources)
            control.last_run_date = datetime.utcnow()
            control.status = request.json.get('status', control.status)
            control.state = request.json.get('state', control.state)
            control.result_investigator = request.json.get('result_investigator', control.result_investigator)
            control.authorized_data = request.json.get('authorized_data', control.authorized_data)
            control.revision += 1
            control.revision_date = datetime.utcnow()
            control.created_by = get_jwt_identity()
            control.creation_date = datetime.utcnow()
            control.last_updated_by = get_jwt_identity()
            control.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Control not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing control', 'error': str(e)}), 500)


@flask_app.route('/def_controls', methods=['DELETE'])
@jwt_required()
def delete_control():
    try:
        def_control_id = request.args.get('def_control_id', type=int)
        if not def_control_id:
            return make_response(jsonify({'message': 'def_control_id is required'}), 400)

        control = DefControl.query.filter_by(def_control_id=def_control_id).first()
        if control:
            db.session.delete(control)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Control not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting control', 'error': str(e)}), 500)


#Workflow engine

# @flask_app.route('/workflow/<int:process_id>', methods=['POST'])
# def api_run_workflow(process_id):
#     """
#     Trigger a workflow by process_id and input parameters.
#     POST body: { ...input parameters... }
#     """
#     try:
#         params = request.get_json() or {}
#         result = run_workflow(process_id, params)
#         return jsonify({"result": result}), 200
#     except Exception as e:
#         traceback.print_exc()
#         return jsonify({"error": str(e)}), 500


# @flask_app.route('/workflow_v1/<int:process_id>', methods=['POST'])
# def api_run_workflow_v1(process_id):
#     """
#     Trigger a workflow by process_id and input parameters.
#     Waits for all tasks to finish and returns the results.
#     """
#     try:
#         params = request.get_json() or {}
#         async_result = run_workflow(process_id, params)  # returns AsyncResult

#         # Wait for workflow to complete
#         final_result = async_result.get(timeout=30)  # adjust timeout as needed

#         return jsonify({"result": final_result}), 200
#     except Exception as e:
#         traceback.print_exc()
#         return jsonify({"error": str(e)}), 500


# @flask_app.route('/workflow_v2/<int:process_id>', methods=['POST'])
# def api_start_workflow_v2(process_id):
#     try:
#         input_params = request.get_json() or {}
#         async_result = run_workflow(process_id, input_params)
#         return jsonify({"task_id": async_result.id}), 202
#     except Exception as e:
#         traceback.print_exc()
#         return jsonify({"error": str(e)}), 500

# @flask_app.route("/workflow_v3/<int:process_id>", methods=["POST"])
# def run_workflow_api(process_id):
#     process = DefProcess.query.filter_by(process_id=process_id).first()
#     if not process:
#         return jsonify({"error": "Process not found"}), 404

#     input_params = request.json or {}
#     celery_task_id = run_workflow(process.process_structure, initial_context=input_params)
#     return jsonify({"status": "started", "celery_task_id": celery_task_id})

# @flask_app.route("/run_workflow_wait/<int:process_id>", methods=["POST"])
# def run_workflow_wait(process_id):
#     process = DefProcess.query.filter_by(process_id=process_id).first()
#     if not process:
#         return jsonify({"error": "Process not found"}), 404

#     input_params = request.json or {}
#     celery_task_id = run_workflow(process.process_structure, initial_context=input_params)
    
#     # Wait for the workflow to finish
#     from celery.result import AsyncResult
#     result = AsyncResult(celery_task_id)
#     workflow_result = result.get(timeout=300)  # Wait max 5 minutes
    
#     return jsonify({"status": "finished", "result": workflow_result})




# @flask_app.route('/get_user_details/<string:user_name>', methods=['GET'])
# def get_user_by_name(user_name):
#     try:
#         user = DefUsersView.query.filter(
#             DefUsersView.user_name.ilike(user_name.strip())
#         ).first()

#         if not user:
#             return make_response(jsonify({
#                 "message": f"User '{user_name}' not found"
#             }), 404)

#         return make_response(jsonify(user.json()), 200)

#     except Exception as e:
#         return make_response(jsonify({
#             "message": "Error retrieving user",
#             "error": str(e)
#         }), 500)


# CELERY FLOWER

@flask_app.route("/flower/tasks", methods=["GET"])
def list_tasks():
    try:
        # res = requests.get(f"{flower_url}/api/tasks", auth=HTTPBasicAuth('user', 'pass'), timeout=5)
        res = requests.get(f"{flower_url}/api/tasks", timeout=5)

        try:
            data = res.json()  # attempt to parse JSON
        except ValueError:
            # If response is not JSON, return raw text
            data = {"error": "Invalid response from Flower", "response_text": res.text}
        return jsonify(data), res.status_code
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Flower service unreachable"}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@flask_app.route("/flower/workers", methods=["GET"])
def list_workers():
    try:
        # res = requests.get(f"{flower_url}/api/workers",auth=HTTPBasicAuth('user', 'pass'), timeout=5)
        res = requests.get(f"{flower_url}/api/workers", timeout=5)

        return jsonify(res.json()), res.status_code
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Flower service unreachable"}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# @flask_app.route("/flower/tasks/types", methods=["GET"])  #doesn't work #404
# def list_task_types():
#     try:
#         res = requests.get(
#             f"{flower_url}/api/tasks/types",
#             auth=HTTPBasicAuth('user', 'pass'),
#             timeout=5
#         )

#         # Debugging info (optional, prints to console)
#         print("Status code:", res.status_code)
#         # print("Response headers:", res.headers)
#         # print("Response body:", repr(res.text))

#         # Handle non-200 status codes
#         if res.status_code != 200:
#             return jsonify({"error": f"Flower API returned status {res.status_code}"}), res.status_code

#         # Handle empty response
#         if not res.text.strip():
#             return jsonify({"error": "Empty response from Flower API"}), 502

#         # Parse JSON safely
#         try:
#             data = res.json()
#         except ValueError:
#             return jsonify({"error": "Invalid JSON response from Flower API"}), 502

#         return jsonify(data), res.status_code

#     except requests.exceptions.ConnectionError:
#         return jsonify({"error": "Flower service unreachable"}), 503
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500



# @flask_app.route("/flower/tasks/types", methods=["GET"])
# def list_task_types():
#     try:
#         res = requests.get(f"{flower_url}/api/tasks/types", auth=HTTPBasicAuth('user', 'pass'), timeout=5)
        
#         # Debugging: Log the response details
#         print("Status code:", res.status_code)
#         print("Response headers:", res.headers)
#         print("Response body:", repr(res.text))
        
#         # Check for non-200 status code
#         if res.status_code != 200:
#             return jsonify({"error": f"Flower API returned status {res.status_code}"}), res.status_code
        
#         # Check for empty response
#         if not res.text.strip():
#             return jsonify({"error": "Empty response from Flower API"}), 502
        
#         # Try parsing JSON
#         try:
#             data = res.json()
#             return jsonify(data), res.status_code
#         except ValueError as e:
#             print(f"JSON parsing error: {e}")
#             return jsonify({"error": f"Invalid JSON response from Flower API: {repr(res.text)}"}), 502
            
#     except requests.exceptions.ConnectionError:
#         return jsonify({"error": "Flower service unreachable"}), 503
#     except Exception as e:
#         print(f"Unexpected error: {e}")
#         return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

@flask_app.route("/flower/task/<task_id>", methods=["GET"])
def get_task_info(task_id):
    try:
        res = requests.get(f"{flower_url}/api/task/info/{task_id}", timeout=5)
        return jsonify(res.json()), res.status_code
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Flower service unreachable"}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

# @flask_app.route("/flower/queues", methods=["GET"])  #404
# def list_queues():
#     try:
#         res = requests.get(f"{flower_url}/api/queues", auth=HTTPBasicAuth('user', 'pass'), timeout=5)

#         # Debug info (optional)
#         print("Status code:", res.status_code)
#         # print("Response headers:", res.headers)
#         # print("Response body:", repr(res.text))

#         if res.status_code != 200:
#             return jsonify({"error": f"Flower API returned status {res.status_code}"}), res.status_code

#         if not res.text.strip():
#             return jsonify({"error": "Empty response from Flower API"}), 502

#         try:
#             data = res.json()
#         except ValueError:
#             return jsonify({"error": "Invalid JSON response from Flower API", "response_text": res.text}), 502

#         return jsonify(data), 200

#     except requests.exceptions.ConnectionError:
#         return jsonify({"error": "Flower service unreachable"}), 503
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

# @flask_app.route("/flower/broker/queues", methods=["GET"]) #404
# def broker_queues():
#     try:
#         res = requests.get(f"{flower_url}/api/broker/queues", auth=HTTPBasicAuth('user', 'pass'), timeout=5)

#         # Debug info (optional)
#         print("Status code:", res.status_code)
#         # print("Response headers:", res.headers)
#         # print("Response body:", repr(res.text))

#         if res.status_code != 200:
#             return jsonify({"error": f"Flower API returned status {res.status_code}"}), res.status_code

#         if not res.text.strip():
#             return jsonify({"error": "Empty response from Flower API"}), 502

#         try:
#             data = res.json()
#         except ValueError:
#             return jsonify({"error": "Invalid JSON response from Flower API", "response_text": res.text}), 502

#         return jsonify(data), 200

#     except requests.exceptions.ConnectionError:
#         return jsonify({"error": "Flower service unreachable"}), 503
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

# @flask_app.route("/flower/tasks/scheduled", methods=["GET"])
# def scheduled_tasks():
#     try:
#         res = requests.get(f"{flower_url}/api/tasks/scheduled", timeout=5)
#         return jsonify(res.json()), res.status_code
#     except requests.exceptions.ConnectionError:
#         return jsonify({"error": "Flower service unreachable"}), 503
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500 

# @flask_app.route("/flower/tasks/active", methods=["GET"])
# def active_tasks():
#     try:
#         res = requests.get(f"{flower_url}/api/tasks/active", timeout=5)
#         return jsonify(res.json()), res.status_code
#     except requests.exceptions.ConnectionError:
#         return jsonify({"error": "Flower service unreachable"}), 503
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

# @flask_app.route("/flower/tasks/reserved", methods=["GET"])
# def reserved_tasks():
#     try:
#         res = requests.get(f"{flower_url}/api/tasks/reserved", timeout=5)
#         return jsonify(res.json()), res.status_code
#     except requests.exceptions.ConnectionError:
#         return jsonify({"error": "Flower service unreachable"}), 503
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500


# @flask_app.route("/flower/tasks/revoked", methods=["GET"])
# def revoked_tasks():
#     try:
#         res = requests.get(f"{flower_url}/api/tasks/revoked", timeout=5)
#         return jsonify(res.json()), res.status_code
#     except requests.exceptions.ConnectionError:
#         return jsonify({"error": "Flower service unreachable"}), 503
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500  






# Create a DefActionItem
@flask_app.route('/def_action_items', methods=['POST'])
@jwt_required()
def create_action_item():
    try:
        action_item_name = request.json.get('action_item_name')
        description = request.json.get('description')
        notification_id = request.json.get('notification_id')
        user_ids = request.json.get('user_ids')
        action = request.json.get('action')

        created_by = get_jwt_identity()

        if not action_item_name:
            return make_response(jsonify({"message": "Action item name is required"}), 400)

        # existing_item = DefActionItem.query.filter_by(action_item_name=action_item_name).first()
        # if existing_item:
        #     return make_response(jsonify({"message": "Action item name already exists"}), 400)

        # if notification_id:
        #     # Optionally validate if notification exists
        #     notification = db.session.query(DefNotification.notification_id).filter_by(notification_id=notification_id).first()
        #     if not notification:
        #         return make_response(jsonify({"message": "Invalid notification_id"}), 400)

        new_action_item = DefActionItem(
            action_item_name = action_item_name,
            description = description,
            created_by = created_by,
            creation_date = datetime.utcnow(),
            last_updated_by = created_by,
            last_update_date = datetime.utcnow(),
            notification_id = notification_id
        )

        db.session.add(new_action_item)
        db.session.flush()  # so we get the ID

        # Add assignments
        if new_action_item:
            for uid in user_ids:
                assignment = DefActionItemAssignment(
                    action_item_id = new_action_item.action_item_id,
                    user_id = uid,
                    status = 'NEW',
                    created_by = get_jwt_identity(),
                    # creation_date = datetime.utcnow(),
                    last_updated_by = get_jwt_identity(),
                    # last_update_date = datetime.utcnow()
                )
                db.session.add(assignment)

    #update action_item_id in def_notifications table
        if new_action_item:
            notification = DefNotifications.query.filter_by(notification_id=notification_id).first()
            if notification:
                notification.action_item_id = new_action_item.action_item_id
            


        db.session.commit()

        if action == 'DRAFT':
            return make_response(jsonify({
                'message': 'Action item saved successfully',
                'result': new_action_item.json()

            }), 201)

        if action == 'SENT':
            return make_response(jsonify({
                'message': 'Action item sent successfully',
                'result': new_action_item.json()
            }), 201)


    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error creating action item", "error": str(e)}), 500)




# Get all DefActionItems
@flask_app.route('/def_action_items', methods=['GET'])
@jwt_required()
def get_action_items():
    try:
        action_items = DefActionItem.query.all()
        if action_items:
            return make_response(jsonify([item.json() for item in action_items]), 200)
        else:
            return make_response(jsonify({"message": "No action items found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving action items", "error": str(e)}), 500)


# Get paginated DefActionItems
@flask_app.route('/def_action_items/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_action_items(page, limit):
    try:
        paginated = DefActionItem.query.order_by(DefActionItem.action_item_id.desc()).paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            'items': [item.json() for item in paginated.items],
            'total': paginated.total,
            'pages': paginated.pages,
            'page': paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching action items', 'error': str(e)}), 500)


# Get a single DefActionItem by ID
@flask_app.route('/def_action_items/<int:action_item_id>', methods=['GET'])
@jwt_required()
def get_action_item(action_item_id):
    try:
        action_item = DefActionItem.query.filter_by(action_item_id=action_item_id).first()
        if action_item:
            return make_response(jsonify(action_item.json()), 200)
        else:
            return make_response(jsonify({"message": "Action item not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving action item", "error": str(e)}), 500)


@flask_app.route('/def_action_items/upsert', methods=['POST'])
@jwt_required()
def upsert_action_item():
    data = request.get_json()
    if not data:
        return jsonify({"message": "Invalid request: No JSON data provided"}), 400

    try:
        action_item_id = data.get('action_item_id')
        user_ids = data.get('user_ids', [])
        status = data.get('status')
        current_user = get_jwt_identity()

        if action_item_id:
            # --- UPDATE ---
            action_item = db.session.get(DefActionItem, action_item_id)
            if not action_item:
                return jsonify({"message": f"Action Item with ID {action_item_id} not found"}), 404

            #Check duplicate name if changing
            # new_name = data.get('action_item_name')
            # if new_name and new_name != action_item.action_item_name:
            #     duplicate = DefActionItem.query.filter_by(action_item_name=new_name).first()
            #     if duplicate:
            #         return jsonify({"message": "Action item name already exists"}), 400
            #     action_item.action_item_name = new_name

            if "action_item_name" in data:
                action_item.action_item_name = data["action_item_name"]

            if "description" in data:
                action_item.description = data["description"]
            if "notification_id" in data:
                action_item.notification_id = data["notification_id"]

            action_item.last_updated_by = current_user
            message = "Edited successfully"
            status_code = 200

        else:
            # --- CREATE ---
            action_item_name = data.get('action_item_name')
            if not action_item_name:
                return jsonify({"message": "Missing required field: action_item_name"}), 400

            # Check duplicate name before insert
            # duplicate = DefActionItem.query.filter_by(action_item_name=action_item_name).first()
            # if duplicate:
            #     return jsonify({"message": "Action item name already exists"}), 400

            action_item = DefActionItem(
                action_item_name = action_item_name,
                description = data.get('description'),
                created_by = current_user,
                creation_date = datetime.utcnow(),
                last_updated_by = current_user,
                last_update_date = datetime.utcnow(),
                notification_id = data.get('notification_id')
            )
            db.session.add(action_item)
            db.session.flush()  # get the ID before assignments
            message = "Added successfully"
            status_code = 201

        db.session.commit()

        # Handle user assignments
        if user_ids:
            # Remove old assignments if updating
            if action_item_id:
                DefActionItemAssignment.query.filter_by(action_item_id=action_item.action_item_id).delete()

            for uid in user_ids:
                assignment = DefActionItemAssignment(
                    action_item_id=action_item.action_item_id,
                    user_id=uid,
                    status=status,
                    created_by=current_user,
                    last_updated_by=current_user
                )
                db.session.add(assignment)

            db.session.commit()

        return make_response(jsonify({
            "message": message,
            "action_item_id": action_item.action_item_id
        }), status_code)

    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "An unexpected error occurred", "error": str(e)}), 500



@flask_app.route('/def_action_items/<int:action_item_id>', methods=['PUT'])
@jwt_required()
def update_action_item(action_item_id):
    try:
        data = request.get_json()
        current_user = get_jwt_identity()

        action_item_name = data.get('action_item_name')
        description = data.get('description')
        notification_id = data.get('notification_id')
        user_ids = data.get('user_ids', [])
        action = data.get('action')
        

        # --- Update DefActionItem main record ---
        action_item = DefActionItem.query.get(action_item_id)
        if not action_item:
            return make_response(jsonify({'message': 'Action Item not found'}), 404)

        if action_item_name:
            action_item.action_item_name = action_item_name
        if description:
            action_item.description = description
        if notification_id:
            action_item.notification_id = notification_id

        action_item.last_updated_by = current_user
        action_item.last_update_date = datetime.utcnow()

        db.session.commit()

        # --- Handle DefActionItemAssignment sync ---
        existing_assignments = DefActionItemAssignment.query.filter_by(
            action_item_id=action_item_id
        ).all()
        existing_user_ids = {a.user_id for a in existing_assignments}
        incoming_user_ids = set(map(int, user_ids))

        # Find differences
        users_to_add = incoming_user_ids - existing_user_ids
        users_to_update = incoming_user_ids & existing_user_ids
        users_to_delete = existing_user_ids - incoming_user_ids

        # Add new recipients
        for uid in users_to_add:
            new_assignment = DefActionItemAssignment(
                action_item_id=action_item_id,
                user_id=uid,
                status='NEW',
                created_by=current_user,
                last_updated_by=current_user
            )
            db.session.add(new_assignment)

        # Update existing recipients
        for uid in users_to_update:
            assignment = DefActionItemAssignment.query.filter_by(
                action_item_id=action_item_id,
                user_id=uid
            ).first()
            if assignment:
                assignment.last_updated_by = current_user
                assignment.last_update_date = datetime.utcnow()

        # Delete removed recipients
        if users_to_delete:
            DefActionItemAssignment.query.filter(
                DefActionItemAssignment.action_item_id == action_item_id,
                DefActionItemAssignment.user_id.in_(users_to_delete)
            ).delete(synchronize_session=False)

        db.session.commit()

        if action == 'DRAFT':
            return make_response(jsonify({
                'message': 'Action item saved successfully',
                'result': action_item.json()
            }), 200)

        if action == 'SENT':
            return make_response(jsonify({
                'message': 'Action item sent successfully',
                'result': action_item.json()
            }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'error': str(e)}), 500)




# Delete a DefActionItem
@flask_app.route('/def_action_items/<int:action_item_id>', methods=['DELETE'])
@jwt_required()
def delete_action_item(action_item_id):
    try:
        action_item = DefActionItem.query.filter_by(action_item_id=action_item_id).first()
        if not action_item:
            return make_response(jsonify({"message": "Action item not found"}), 404)

        # First delete all related assignments
        DefActionItemAssignment.query.filter_by(action_item_id=action_item_id).delete()

        # Then delete the main action item
        db.session.delete(action_item)
        db.session.commit()

        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error deleting action item", "error": str(e)}), 500)




# Create DefActionItemAssignments (multiple user_ids)
@flask_app.route('/def_action_item_assignments', methods=['POST'])
@jwt_required()
def create_action_item_assignments():
    try:
        action_item_id = request.json.get('action_item_id')
        user_ids = request.json.get('user_ids')
        status = request.json.get('status')


        if not action_item_id:
            return make_response(jsonify({"message": "action_item_id is required"}), 400)
        if not user_ids or not isinstance(user_ids, list):
            return make_response(jsonify({"message": "user_ids must be a non-empty list"}), 400)

        created_assignments = []
        for uid in user_ids:
            assignment = DefActionItemAssignment(
                action_item_id = action_item_id,
                user_id = uid,
                status = status,
                created_by = get_jwt_identity(),
                creation_date = datetime.utcnow(),
                last_updated_by = get_jwt_identity(),
                last_update_date = datetime.utcnow()
            )
            db.session.add(assignment)
            created_assignments.append(assignment)

        db.session.commit()
        return make_response(jsonify({"message": "Added successfully"}), 201)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({"message": "One or more assignments already exist"}), 400)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error creating assignments", "error": str(e)}), 500)


# Get all DefActionItemAssignments
@flask_app.route('/def_action_item_assignments', methods=['GET'])
@jwt_required()
def get_action_item_assignments():
    try:
        assignments = DefActionItemAssignment.query.order_by(DefActionItemAssignment.action_item_id.desc()).all()
        if assignments:
            return make_response(jsonify([a.json() for a in assignments]), 200)
        else:
            return make_response(jsonify({"message": "No assignments found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving assignments", "error": str(e)}), 500)



# Update DefActionItemAssignments (replace user_ids for given action_item_id)
@flask_app.route('/def_action_items/update_status/<int:user_id>/<int:action_item_id>', methods=['PUT'])
@jwt_required()
def update_action_item_assignment_status(user_id, action_item_id):
    try:
        data = request.get_json()
        if not data or 'status' not in data:
            return make_response(jsonify({"message": "Missing required field: status"}), 400)

        # Fetch the assignment
        assignment = DefActionItemAssignment.query.filter_by(
            action_item_id=action_item_id,
            user_id=user_id
        ).first()

        if not assignment:
            return make_response(jsonify({"message": "Assignment not found"}), 404)

        # Update only the status
        assignment.status = data['status']
        assignment.last_updated_by = get_jwt_identity()
        assignment.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({"message": "Status Updated Successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error updating status", "error": str(e)}), 500)



# Delete a single DefActionItemAssignment
@flask_app.route('/def_action_item_assignments/<int:user_id>/<int:action_item_id>', methods=['DELETE'])
@jwt_required()
def delete_action_item_assignment(action_item_id, user_id):
    try:
        assignment = DefActionItemAssignment.query.filter_by(
            action_item_id=action_item_id,
            user_id=user_id
        ).first()
        if assignment:
            db.session.delete(assignment)
            db.session.commit()
            return make_response(jsonify({"message": "Deleted successfully"}), 200)
        return make_response(jsonify({"message": "Assignment not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error deleting assignment", "error": str(e)}), 500)


@flask_app.route('/def_action_items_view/<int:user_id>/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_action_items_view(user_id, page, limit):
    try:
        status = request.args.get('status')
        action_item_name = request.args.get('action_item_name', '').strip()
        search_underscore = action_item_name.replace(' ', '_')
        search_space = action_item_name.replace('_', ' ')

        # Validate pagination
        if page < 1 or limit < 1:
            return make_response(jsonify({
                "message": "Page and limit must be positive integers"
            }), 400)

        # Base 
        # query = DefActionItemsV.query.filter_by(user_id=user_id)

        # Base query: filter by user_id and only SENT status
        query = DefActionItemsV.query.filter_by(user_id=user_id).filter(
            func.lower(func.trim(DefActionItemsV.notification_status)) == "sent"
        )

        #Apply status filter if provided
        if status:
            query = query.filter(
                func.lower(func.trim(DefActionItemsV.status)) == func.lower(func.trim(status))
            )

        if action_item_name:
            query = query.filter(
                or_(
                    DefActionItemsV.action_item_name.ilike(f'%{action_item_name}%'),
                    DefActionItemsV.action_item_name.ilike(f'%{search_underscore}%'),
                    DefActionItemsV.action_item_name.ilike(f'%{search_space}%')
                )
            )

        query = query.order_by(DefActionItemsV.action_item_id.desc())

        
        # paginated
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

        # Without pagination
        
        # items = query.all()
        # return make_response(jsonify({
        #     "items": [item.json() for item in items],
        #     "total": len(items)
        # }), 200)

    except Exception as e:
        return make_response(jsonify({
            'message': 'Error fetching action items view',
            'error': str(e)
        }), 500)


@flask_app.route('/def_action_items_view/<int:user_id>/<string:status>/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_action_items_by_status(user_id, status, page, limit):
    try:
        # Validate pagination
        if page < 1 or limit < 1:
            return make_response(jsonify({
                "message": "Page and limit must be positive integers"
            }), 400)

        # Query filtered by user_id + status (case-insensitive, trim)
        query = DefActionItemsV.query.filter(
            DefActionItemsV.user_id == user_id,
            func.lower(func.trim(DefActionItemsV.status)) == func.lower(func.trim(status)),
            func.lower(func.trim(DefActionItemsV.notification_status)) == "sent"
        ).order_by(DefActionItemsV.action_item_id.desc())

        # Pagination
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)



@flask_app.route('/view_requests_v4/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def combined_tasks_v4(page, limit):
    try:
        days = request.args.get('days', type=int)
        search_query = request.args.get('task_name', '').strip().lower()

        query = DefAsyncTaskRequest.query

        if search_query and days is None:
            days = 7

        if days is not None:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            query = query.filter(DefAsyncTaskRequest.creation_date >= cutoff_date)

        if search_query:
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(or_(
                DefAsyncTaskRequest.task_name.ilike(f'%{search_query}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_space}%')
            ))

        paginated = query.order_by(DefAsyncTaskRequest.creation_date.desc()) \
                         .paginate(page=page, per_page=limit, error_out=False)

        db_tasks = paginated.items

        # db_tasks = query.order_by(DefAsyncTaskRequest.creation_date.desc()).all()
        db_task_ids = {t.task_id for t in db_tasks if t.task_id}

        flower_tasks = {}
        try:
            res = requests.get(f"{flower_url}/api/tasks", timeout=5)
            if res.status_code == 200:
                flower_tasks = res.json()
        except:
            pass  # keep flower_tasks empty if error

        items = []


        # Add Flower tasks first (skip duplicates if already in DB)
        for tid, ftask in flower_tasks.items():
            # if tid in db_task_ids:
            #     continue  # skip duplicate
            if tid in db_task_ids or ftask.get("state", "").lower() == "success":
                continue 

            args_list = []
            try:
                args_list = ast.literal_eval(ftask.get("args", "[]"))  # safely parse string repr
            except Exception:
                args_list = []

            script_name          = args_list[0] if len(args_list) > 0 else None
            user_task_name       = args_list[1] if len(args_list) > 1 else None
            task_name            = args_list[2] if len(args_list) > 2 else None
            user_schedule_name   = args_list[3] if len(args_list) > 3 else None
            redbeat_schedule_name= args_list[4] if len(args_list) > 4 else None
            schedule_type        = args_list[5] if len(args_list) > 5 else None
            schedule_data        = args_list[6] if len(args_list) > 6 else None

            items.append({
                "uuid": ftask.get("uuid"),
                "task_id": tid,
                "script_name": script_name,
                "user_task_name": user_task_name,
                "task_name": task_name,                   
                "user_schedule_name": user_schedule_name,
                "redbeat_schedule_name": redbeat_schedule_name,
                "schedule_type": schedule_type,
                "schedule": schedule_data,
                "state": ftask.get("state"),
                "worker": ftask.get("worker"),
                # "timestamp": ftask.get("timestamp"),
                "result": ftask.get("result"),
                "args": ftask.get("args"),                 
                "kwargs": ftask.get("kwargs"),
                "parameters": None,                        # DB-only field
                "request_id": None,
                "executor": None,
                "created_by": None,
                "creation_date": None,
                "last_updated_by": None,
                "last_updated_date": None
            })

        
        for t in db_tasks:
            item = t.json()  # get all DB fields
            # add Flower fields
            if t.task_id and t.task_id in flower_tasks:
                ftask = flower_tasks[t.task_id]
                item["uuid"] = ftask.get("uuid")
                item["state"] = ftask.get("state")
                item["worker"] = ftask.get("worker")
            else:
                item["uuid"] = None
                item["state"] = None
                item["worker"] = None
            items.append(item)

        
        

        return make_response(jsonify({
            "items": items,
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    

        # --- Manual pagination on merged list ---
        # total = len(items)
        # start = (page - 1) * limit
        # end = start + limit
        # paginated_items = items[start:end]
        # pages = (total + limit - 1) // limit  # ceil division


        # return make_response(jsonify({
        #     "items": paginated_items,
        #     "total": total,
        #     "pages": pages,
        #     "page": page
        # }), 200)

    except Exception as e:
        return jsonify({"error": str(e)}), 500










@flask_app.route('/def_control_environments', methods=['GET'])
@jwt_required()
def get_control_environments():
    try:
        # Query params
        page = request.args.get("page", type=int)
        limit = request.args.get("limit", type=int)
        control_environment_id = request.args.get("control_environment_id", type=int)
        name = request.args.get("name", "").strip()

        # Validate pagination
        if page is not None and limit is not None:
            if page < 1 or limit < 1:
                return make_response(jsonify({
                    "message": "Page and limit must be positive integers"
                }), 400)

        # Base query
        query = DefControlEnvironment.query

        # Filter by control_environment_id
        if control_environment_id:
            query = query.filter(DefControlEnvironment.control_environment_id == control_environment_id)

        # Filter by name (supports underscores/spaces)
        if name:
            search_underscore = name.replace(" ", "_")
            search_space = name.replace("_", " ")
            query = query.filter(
                or_(
                    DefControlEnvironment.name.ilike(f"%{name}%"),
                    DefControlEnvironment.name.ilike(f"%{search_underscore}%"),
                    DefControlEnvironment.name.ilike(f"%{search_space}%"),
                )
            )

        # Order by latest first
        query = query.order_by(DefControlEnvironment.control_environment_id.desc())

        # Paginated response
        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            items = [env.json() for env in paginated.items]
            return make_response(jsonify({
                "items": items,
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)

        # Return all if no pagination
        environments = query.all()
        if environments:
            return make_response(jsonify([env.json() for env in environments]), 200)
        else:
            return make_response(jsonify({"message": "No control environments found"}), 404)

    except Exception as e:
        return make_response(
            jsonify({
                "message": "Error retrieving control environments",
                "error": str(e)
            }),
            500
        )




@flask_app.route('/def_control_environments', methods=['POST'])
@jwt_required()
def create_control_environment():
    try:
        data = request.get_json()
        current_user = get_jwt_identity()

        if not data or "name" not in data:
            return make_response(jsonify({"message": "Missing required field: name"}), 400)

        new_env = DefControlEnvironment(
            name=data.get("name"),
            description=data.get("description"),
            created_by=current_user,
            last_updated_by=current_user,
        )

        db.session.add(new_env)
        db.session.commit()

        return make_response(jsonify({"message": "Added successfully",
                                      "result": new_env.json() }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(
            jsonify({
                "message": "Error creating control environment",
                "error": str(e)
            }),
            500
        )




@flask_app.route('/def_control_environments', methods=['PUT'])
@jwt_required()
def update_control_environment():
    try:
        control_environment_id = request.args.get('control_environment_id', type=int)
        if not control_environment_id:
            return make_response(jsonify({"message": "Control Environment ID is required"}), 400)

        data = request.get_json()


        env = DefControlEnvironment.query.filter_by(control_environment_id=control_environment_id).first()
        if not env:
            return make_response(jsonify({"message": "Control environment not found"}), 404)


        if env:
            env.name = data.get("name", env.name)
            env.description = data.get("description", env.description)
            env.last_updated_by = get_jwt_identity()



            db.session.commit()

        return make_response(jsonify({
            "message": "Edited successfully",
            "result": env.json()
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "message": "Error updating control environment",
            "error": str(e)
        }), 500)



@flask_app.route('/def_control_environments', methods=['DELETE'])
@jwt_required()
def delete_control_environments():
    try:
        data = request.get_json()
        environment_ids = data.get("control_environment_ids", []) if data else []

        if not environment_ids:
            return make_response(jsonify({"message": "Environment IDs are required"}), 400)

        envs = DefControlEnvironment.query.filter(
            DefControlEnvironment.control_environment_id.in_(environment_ids)
        ).all()

        if not envs:
            return make_response(jsonify({"message": "No matching control environments found"}), 404)

        for env in envs:
            db.session.delete(env)

        db.session.commit()
        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error deleting control environments", "error": str(e)}), 500)



#INVITATION

# @flask_app.route("/invitation/via_email", methods=["POST"])
# @jwt_required()
# def invitation_via_email():
#     """Send invitation via email"""
#     try:
#         current_user = get_jwt_identity()
#         data = request.get_json() or {}
#         invited_by = data.get("invited_by") or current_user
#         email = data.get("email")

#         if not invited_by or not email:
#             return jsonify({"error": "Inviter ID and email required"}), 400

#         # Check if user already exists
#         if DefUser.query.filter_by(email_address=email).first():
#             return jsonify({"message": "User with this email already exists"}), 200

#         # Token expiration and generation
#         expires = timedelta(hours=invitation_expire_time)
#         token = create_access_token(identity=str(invited_by), expires_delta=expires)

#         # Check for existing pending invite
#         existing_invite = NewUserInvitation.query.filter_by(
#             email=email, status="PENDING", type="EMAIL"
#         ).first()

#         if existing_invite and existing_invite.expires_at > datetime.utcnow():
#             encrypted_id = serializer.dumps(str(existing_invite.user_invitation_id))
#             invite_link = f"{request.host_url}invitation/{encrypted_id}/{existing_invite.token}"
#             return jsonify({
#                 "invitation_id": existing_invite.user_invitation_id,
#                 "token": existing_invite.token,
#                 "invitation_link": invite_link,
#                 "message": "Pending invitation already exists"
#             }), 200
#         elif existing_invite:
#             existing_invite.status = "EXPIRED"
#             db.session.commit()

#         # Create new invitation
#         expires_at = datetime.utcnow() + expires
#         new_invite = NewUserInvitation(
#             invited_by=invited_by,
#             email=email,
#             token=token,
#             status="PENDING",
#             type="EMAIL",
#             created_at=current_timestamp(),
#             expires_at=expires_at
#         )
#         db.session.add(new_invite)
#         db.session.flush()

#         encrypted_id = serializer.dumps(str(new_invite.user_invitation_id))
#         invite_link = f"{request.host_url}invitation/{encrypted_id}/{token}"

#         # Send email
#         msg = MailMessage(
#             subject="You're Invited to Join PROCG",
#             recipients=[email],
#             html=f"""
#             <p>Hello,</p>
#             <p>You’ve been invited to join PROCG!</p>
#             <p>Click below to accept your invitation:</p>
#             <p><a href="{invite_link}">Accept Invitation</a></p>
#             <p>This link expires in {invitation_expire_time} hour(s).</p>
#             <p>— The PROCG Team</p>
#             """
#         )

#         try:
#             mail.send(msg)
#         except Exception as mail_error:
#             db.session.rollback()
#             return jsonify({"error": str(mail_error), "message": "Failed to send email."}), 500

#         db.session.commit()

#         return jsonify({
#             "success": True,
#             "invitation_id": new_invite.user_invitation_id,
#             "token": token,
#             "invitation_link": invite_link,
#             "message": "Invitation email sent successfully"
#         }), 201

#     except Exception as e:
#         db.session.rollback()
#         return jsonify({"error": str(e), "message": "Failed to send invitation"}), 500


# @flask_app.route("/invitation/via_link", methods=["POST"])
# @jwt_required()
# def invitation_via_link():
#     """Generate invitation link only"""
#     try:
#         current_user = get_jwt_identity()
#         invited_by = current_user

#         if not invited_by:
#             return jsonify({"error": "Inviter ID required"}), 400

#         expires = timedelta(hours=invitation_expire_time)
#         token = create_access_token(identity=str(invited_by), expires_delta=expires)
#         expires_at = datetime.utcnow() + expires

#         new_invite = NewUserInvitation(
#             invited_by=invited_by,
#             token=token,
#             status="PENDING",
#             type="LINK",
#             created_at=current_timestamp(),
#             expires_at=expires_at
#         )
#         db.session.add(new_invite)
#         db.session.commit()

#         encrypted_id = serializer.dumps(str(new_invite.user_invitation_id))
#         invite_link = f"{request.host_url}invitation/{encrypted_id}/{token}"

#         return jsonify({
#             "success": True,
#             "invitation_link": invite_link,
#             "message": "The invitation link was generated successfully"
#         }), 201

#     except Exception as e:
#         db.session.rollback()
#         return jsonify({"error": str(e)}), 500


# @flask_app.route("/invitation/<string:encrypted_id>/<string:token>/verify", methods=["GET"])
# def verify_invitation(encrypted_id, token):
#     try:
#         try:
#             invitation_id = int(serializer.loads(encrypted_id, max_age=invitation_expire_time * 3600))
#         except SignatureExpired:
#             return jsonify({"valid": False, "message": "Invitation expired"}), 401
#         except BadSignature:
#             return jsonify({"valid": False, "message": "Invalid invitation ID"}), 403

#         try:
#             decoded = decode_token(token)
#             invited_by = decoded.get("sub")
#         except Exception as e:
#             msg = str(e).lower()
#             if "expired" in msg:
#                 return jsonify({"valid": False, "message": "Token expired"}), 401
#             return jsonify({"valid": False, "message": "Invalid token"}), 403

#         invite = NewUserInvitation.query.filter_by(
#             user_invitation_id=invitation_id,
#             invited_by=invited_by,
#             token=token
#         ).first()

#         if not invite:
#             return jsonify({"valid": False, "message": "No invitation found"}), 404

#         if invite.status != "PENDING" or invite.expires_at < datetime.utcnow():
#             invite.status = "EXPIRED"
#             db.session.commit()
#             return jsonify({"valid": False, "message": "Invitation expired"}), 200

#         return jsonify({
#             "valid": True,
#             "invited_by": invite.invited_by,
#             "email": invite.email,
#             "type": invite.type,
#             "message": "Invitation link is valid"
#         }), 200

#     except Exception as e:
#         return jsonify({"valid": False, "message": str(e)}), 500

# @flask_app.route("/invitation/<encrypted_id>/<token>/accept", methods=["POST"])
# def accept_invitation(encrypted_id, token):
#     try:
#         # Decrypt invitation ID
#         try:
#             user_invitation_id = int(serializer.loads(encrypted_id))
#         except (BadSignature, SignatureExpired):
#             return jsonify({"message": "Invalid or expired invitation link"}), 400

#         data = request.get_json() or {}
#         required_fields = ["user_name", "user_type", "email_address", "tenant_id", "password"]
#         missing = [f for f in required_fields if not data.get(f)]
#         if missing:
#             return jsonify({"message": f"Missing required fields: {', '.join(missing)}"}), 400

#         # Decode JWT token
#         try:
#             decoded = decode_token(token)
#         except Exception as e:
#             msg = str(e).lower()
#             if "expired" in msg:
#                 return jsonify({"message": "Token has expired"}), 401
#             return jsonify({"message": "Invalid token"}), 403

#         inviter_id = decoded.get("sub")
#         if not inviter_id:
#             return jsonify({"message": "Missing inviter info in token"}), 403

#         # Check invitation
#         invite = NewUserInvitation.query.filter_by(
#             user_invitation_id=user_invitation_id,
#             invited_by=inviter_id,
#             token=token
#         ).first()

#         if not invite or invite.status in ["ACCEPTED", "EXPIRED"] or (invite.expires_at and invite.expires_at < datetime.utcnow()):
#             if invite:
#                 invite.status = "EXPIRED"
#                 db.session.commit()
#             return jsonify({"message": "This invitation is not valid"}), 400

#         # Check existing username/email
#         if DefUser.query.filter_by(user_name=data["user_name"]).first():
#             return jsonify({"message": "Username already exists"}), 409
#         if DefUser.query.filter_by(email_address=data["email_address"]).first():
#             return jsonify({"message": "Email already exists"}), 409

#         # Create user
#         new_user = DefUser(
#             user_name=data["user_name"],
#             user_type=data["user_type"],
#             email_address=data["email_address"],
#             tenant_id=data["tenant_id"],
#             created_by=inviter_id,
#             last_updated_by=inviter_id,
#             created_on=current_timestamp(),
#             last_updated_on=current_timestamp(),
#             user_invitation_id=user_invitation_id,
#             profile_picture=data.get("profile_picture") or {
#                 "original": "uploads/profiles/default/profile.jpg",
#                 "thumbnail": "uploads/profiles/default/thumbnail.jpg"
#             }
#         )
#         db.session.add(new_user)
#         db.session.flush()  # get new_user.user_id

#         # Create person if user_type is person
#         if data["user_type"].lower() == "person":
#             new_person = DefPerson(
#                 user_id=new_user.user_id,
#                 first_name=data.get("first_name"),
#                 middle_name=data.get("middle_name"),
#                 last_name=data.get("last_name"),
#                 job_title=data.get("job_title")
#             )
#             db.session.add(new_person)

#         # Create credentials
#         hashed_password = generate_password_hash(data["password"], method="pbkdf2:sha256", salt_length=16)
#         new_cred = DefUserCredential(
#             user_id=new_user.user_id,
#             password=hashed_password
#         )
#         db.session.add(new_cred)

#         # Update invitation
#         invite.registered_user_id = new_user.user_id
#         invite.status = "ACCEPTED"
#         invite.accepted_at = current_timestamp()

#         db.session.commit()

#         return jsonify({"message": "Invitation accepted, user created successfully", "user_id": new_user.user_id}), 201

#     except Exception as e:
#         db.session.rollback()
#         return jsonify({"message": "Error processing invitation", "error": str(e)}), 500





#-----------------RBAC--------------

@flask_app.route('/def_privileges', methods=['GET'])
@jwt_required()
def get_def_privileges():
    try:
        privilege_id = request.args.get("privilege_id", type=int)

        # Single-record lookup if privilege_id is provided
        if privilege_id is not None:
            record = DefPrivilege.query.get(privilege_id)
            if not record:
                return make_response(jsonify({
                    "error": f"Privilege with id={privilege_id} not found"
                }), 404)
            return make_response(jsonify(record.json()), 200)

        # Otherwise return all records
        records = DefPrivilege.query.order_by(DefPrivilege.privilege_id.desc()).all()
        return make_response(jsonify([r.json() for r in records]), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching privileges"
        }), 500)




@flask_app.route('/def_privileges', methods=['POST'])
@jwt_required()
def create_def_privilege():
    try:
        data = request.get_json()
        privilege_id = request.json.get('privilege_id')
        privilege_name = data.get('privilege_name')

        if not privilege_id:
            return make_response(jsonify({'error': 'privilege_id is required'}), 400)

        if not privilege_name:
            return make_response({"error": "privilege_name is required"}, 400)
        
        existing = DefPrivilege.query.filter_by(privilege_id=privilege_id).first()
        if existing:
            return make_response(jsonify({'error': 'privilege_id already exists'}), 400)

        new_record = DefPrivilege(
            privilege_id=privilege_id,
            privilege_name=privilege_name,
            created_by=get_jwt_identity(),
            creation_date=datetime.utcnow(),
        )

        db.session.add(new_record)
        db.session.commit()

        return make_response(new_record.json(), 201)

    except Exception as e:
        db.session.rollback()
        return make_response({"error": str(e)}, 500)



@flask_app.route('/def_privileges', methods=['PUT'])
@jwt_required()
def update_privilege():
    try:
        privilege_id = request.args.get("privilege_id", type=int)
        if privilege_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'privilege_id' is required"
            }), 400)
        
        privilege_name = request.json.get('privilege_name')
        # updated_by = get_jwt_identity()

        privilege = DefPrivilege.query.filter_by(privilege_id=privilege_id).first()
        if not privilege:
            return make_response(jsonify({'error': 'Privilege not found'}), 404)

        if privilege_name:
            privilege.privilege_name = privilege_name

        privilege.last_updated_by = get_jwt_identity()
        privilege.last_update_date = datetime.utcnow()

        db.session.commit()

        return make_response(jsonify({'message': 'Edited successfully'}), 200)

    except Exception as e:
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error updating privilege'
        }), 500)


@flask_app.route('/def_privileges', methods=['DELETE'])
@jwt_required()
def delete_privilege():
    try:
        privilege_id = request.args.get("privilege_id", type=int)
        if privilege_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'privilege_id' is required"
            }), 400)
        
        privilege = DefPrivilege.query.filter_by(privilege_id=privilege_id).first()

        if not privilege:
            return make_response(jsonify({'error': 'Privilege not found'}), 404)

        db.session.delete(privilege)
        db.session.commit()

        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error deleting privilege'
        }), 500)





@flask_app.route('/def_roles', methods=['POST'])
@jwt_required()
def create_role():
    try:
        role_id = request.json.get('role_id')
        role_name = request.json.get('role_name')

        if not role_id:
            return make_response(jsonify({'error': 'role_id is required'}), 400)

        if not role_name:
            return make_response(jsonify({'error': 'role_name is required'}), 400)

        existing = DefRoles.query.filter_by(role_id=role_id).first()
        if existing:
            return make_response(jsonify({'error': 'role_id already exists'}), 400)

        new_role = DefRoles(
            role_id=role_id,
            role_name=role_name,
            created_by     = get_jwt_identity(),
            creation_date  = datetime.utcnow(),
        )

        db.session.add(new_role)
        db.session.commit()

        return make_response(jsonify({'message': 'Added successfully'}), 201)

    except Exception as e:
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error creating role'
        }), 500)



@flask_app.route('/def_roles', methods=['GET'])
@jwt_required()
def get_roles():
    try:
        role_id = request.args.get("role_id", type=int)

        # Single-role lookup if role_id is provided
        if role_id is not None:
            role = DefRoles.query.filter_by(role_id=role_id).first()
            if not role:
                return make_response(jsonify({
                    "error": f"Role with id={role_id} not found"
                }), 404)
            return make_response(jsonify(role.json()), 200)

        # Otherwise return all roles
        roles = DefRoles.query.order_by(DefRoles.role_id.desc()).all()
        return make_response(jsonify([r.json() for r in roles]), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching roles"
        }), 500)



@flask_app.route('/def_roles', methods=['PUT'])
@jwt_required()
def update_role():
    try:
        role_id = request.args.get("role_id", type=int)
        if role_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'role_id' is required"
            }), 400)
        role_name = request.json.get('role_name')

        role = DefRoles.query.filter_by(role_id=role_id).first()
        if not role:
            return make_response(jsonify({'error': 'Role not found'}), 404)

        if role_name:
            role.role_name = role_name


        role.last_updated_by = get_jwt_identity()
        role.last_update_date = datetime.utcnow()

        db.session.commit()

        return make_response(jsonify({'message': 'Edited successfully'}), 200)

    except Exception as e:
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error updating role'
        }), 500)


@flask_app.route('/def_roles', methods=['DELETE'])
@jwt_required()
def delete_role():
    try:
        role_id = request.args.get("role_id", type=int)
        if role_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'role_id' is required"
            }), 400)

        role = DefRoles.query.filter_by(role_id=role_id).first()

        if not role:
            return make_response(jsonify({'error': 'Role not found'}), 404)

        db.session.delete(role)
        db.session.commit()

        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        return make_response(jsonify({
            'error': str(e),
            'message': 'Error deleting role'
        }), 500)




@flask_app.route('/def_api_endpoints', methods=['POST'])
@jwt_required()
def create_api_endpoint():
    try:
        api_endpoint_id = request.json.get('api_endpoint_id')
        api_endpoint = request.json.get('api_endpoint')
        parameter1 = request.json.get('parameter1')
        parameter2 = request.json.get('parameter2')
        method = request.json.get('method')
        privilege_id = request.json.get('privilege_id')


        if not api_endpoint_id:
            return make_response(jsonify({'error': 'api_endpoint_id is required'}), 400)

        if DefApiEndpoint.query.filter_by(api_endpoint_id=api_endpoint_id).first():
            return make_response(jsonify({'error': 'api_endpoint_id already exists'}), 400)

        # FK validation
        if privilege_id and not DefPrivilege.query.filter_by(privilege_id=privilege_id).first():
            return make_response(jsonify({'error': 'privilege_id not found'}), 404)

        new_api = DefApiEndpoint(
            api_endpoint_id=api_endpoint_id,
            api_endpoint=api_endpoint,
            parameter1=parameter1,
            parameter2=parameter2,
            method=method,
            privilege_id=privilege_id,
            created_by     = get_jwt_identity(),
            creation_date  = datetime.utcnow(),
            last_updated_by = get_jwt_identity(),
            last_update_date = datetime.utcnow()
        )

        db.session.add(new_api)
        db.session.commit()

        return make_response(jsonify({'message': 'Added successfully'}), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e)}), 500)





@flask_app.route('/def_api_endpoints', methods=['GET'])
@jwt_required()
def get_api_endpoints():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)

        # Single-record lookup if api_endpoint_id is provided
        if api_endpoint_id is not None:
            endpoint = DefApiEndpoint.query.filter_by(api_endpoint_id=api_endpoint_id).first()
            if not endpoint:
                return make_response(jsonify({
                    "error": f"API endpoint with id={api_endpoint_id} not found"
                }), 404)
            return make_response(jsonify(endpoint.json()), 200)

        # Otherwise return all endpoints
        endpoints = DefApiEndpoint.query.order_by(DefApiEndpoint.api_endpoint_id.desc()).all()
        return make_response(jsonify([e.json() for e in endpoints]), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching API endpoints"
        }), 500)



@flask_app.route('/def_api_endpoints', methods=['PUT'])
@jwt_required()
def update_api_endpoint():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)

        # Validate required param
        if api_endpoint_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'api_endpoint_id' is required"
            }), 400)
        
        row = DefApiEndpoint.query.filter_by(api_endpoint_id=api_endpoint_id).first()
        if not row:
            return make_response(jsonify({'error': 'API endpoint not found'}), 404)

        row.api_endpoint = request.json.get('api_endpoint', row.api_endpoint)
        row.parameter1 = request.json.get('parameter1', row.parameter1)
        row.parameter2 = request.json.get('parameter2', row.parameter2)
        row.method = request.json.get('method', row.method)

        privilege_id = request.json.get('privilege_id', row.privilege_id)
        if privilege_id and not DefPrivilege.query.filter_by(privilege_id=privilege_id).first():
            return make_response(jsonify({'error': 'privilege_id not found'}), 404)
        row.privilege_id = privilege_id

        row.last_updated_by = get_jwt_identity()
        row.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({'message': 'Edited successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e)}), 500)



@flask_app.route('/def_api_endpoints', methods=['DELETE'])
@jwt_required()
def delete_api_endpoint():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)

        # Validate required param
        if api_endpoint_id is None:
            return make_response(jsonify({
                "error": "Query parameter 'api_endpoint_id' is required"
            }), 400)
        
        row = DefApiEndpoint.query.filter_by(api_endpoint_id=api_endpoint_id).first()
        if not row:
            return make_response(jsonify({'error': 'API endpoint not found'}), 404)

        db.session.delete(row)
        db.session.commit()

        return make_response(jsonify({'message': 'Deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"error": str(e)}), 500)




@flask_app.route('/def_api_endpoint_roles', methods=['POST'])
@jwt_required()
def create_api_endpoint_role():
    try:
        api_endpoint_id = request.json.get('api_endpoint_id')
        role_id = request.json.get('role_id')


        # Validation
        if not api_endpoint_id or not role_id:
            return make_response(jsonify({
                "error": "api_endpoint_id and role_id are required"
            }), 400)

        # FK Check: API Endpoint
        endpoint = DefApiEndpoint.query.filter_by(api_endpoint_id=api_endpoint_id).first()
        if not endpoint:
            return make_response(jsonify({
                "error": f"API Endpoint ID {api_endpoint_id} does not exist"
            }), 404)

        # FK Check: Role
        role = DefRoles.query.filter_by(role_id=role_id).first()
        if not role:
            return make_response(jsonify({
                "error": f"Role ID {role_id} does not exist"
            }), 404)

        # Check duplicate
        existing = DefApiEndpointRole.query.filter_by(
            api_endpoint_id=api_endpoint_id,
            role_id=role_id
        ).first()
        if existing:
            return make_response(jsonify({
                "error": "Mapping already exists"
            }), 409)

        new_mapping = DefApiEndpointRole(
            api_endpoint_id=api_endpoint_id,
            role_id=role_id,
            created_by     = get_jwt_identity(),
            creation_date  = datetime.utcnow()

        )

        db.session.add(new_mapping)
        db.session.commit()

        return make_response(jsonify(new_mapping.json()), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error creating API endpoint role"
        }), 500)


@flask_app.route('/def_api_endpoint_roles', methods=['GET'])
@jwt_required()
def get_api_endpoint_roles():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)
        role_id = request.args.get("role_id", type=int)

        # If both provided -> single-record lookup
        if api_endpoint_id is not None and role_id is not None:
            record = DefApiEndpointRole.query.filter_by(
                api_endpoint_id=api_endpoint_id,
                role_id=role_id
            ).first()
            if not record:
                return make_response(jsonify({
                    "error": f"No data found for api_endpoint_id={api_endpoint_id} and role_id={role_id}"
                }), 404)
            return make_response(jsonify(record.json()), 200)

        # Otherwise build a list query (may be empty)
        query = DefApiEndpointRole.query

        if api_endpoint_id is not None:
            query = query.filter_by(api_endpoint_id=api_endpoint_id)
        if role_id is not None:
            query = query.filter_by(role_id=role_id)

        records = query.order_by(DefApiEndpointRole.creation_date.desc()).all()
        return make_response(jsonify([r.json() for r in records]), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching API endpoint roles"
        }), 500)





@flask_app.route('/def_api_endpoint_roles', methods=['PUT'])
@jwt_required()
def update_api_endpoint_role():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)
        role_id = request.args.get("role_id", type=int)

        # Validate required params
        if api_endpoint_id is None or role_id is None:
            return make_response(jsonify({
                "error": "Query parameters 'api_endpoint_id' and 'role_id' are required"
            }), 400)

        record = DefApiEndpointRole.query.filter_by(
            api_endpoint_id=api_endpoint_id,
            role_id=role_id
        ).first()

        if not record:
            return make_response(jsonify({
                "error": "Record not found",
                "message": "API Endpoint-Role mapping does not exist"
            }), 404)
        
        record.api_endpoint_id = request.json.get('api_endpoint_id', record.api_endpoint_id)
        record.role_id = request.json.get('role_id', record.role_id)

        record.last_updated_by = get_jwt_identity()
        record.last_update_date = datetime.utcnow()

        db.session.commit()

        return make_response(jsonify({
            "message": "Edited successfully",
            "data": record.json()
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error updating API endpoint-role mapping"
        }), 500)



@flask_app.route('/def_api_endpoint_roles', methods=['DELETE'])
@jwt_required()
def delete_api_endpoint_role():
    try:
        api_endpoint_id = request.args.get("api_endpoint_id", type=int)
        role_id = request.args.get("role_id", type=int)

        # Validate required params
        if api_endpoint_id is None or role_id is None:
            return make_response(jsonify({
                "error": "Query parameters 'api_endpoint_id' and 'role_id' are required"
            }), 400)

        # Find the record
        record = DefApiEndpointRole.query.filter_by(
            api_endpoint_id=api_endpoint_id,
            role_id=role_id
        ).first()

        if not record:
            return make_response(jsonify({
                "error": f"No mapping found for api_endpoint_id={api_endpoint_id} and role_id={role_id}"
            }), 404)

        db.session.delete(record)
        db.session.commit()

        return make_response(jsonify({
            "message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error deleting API endpoint-role"
        }), 500)






@flask_app.route('/def_user_granted_roles', methods=['POST'])
@jwt_required()
def create_user_granted_roles():
    try:
        data = request.json
        user_id = data.get('user_id')
        role_ids = data.get('role_ids')

        # Validate input
        if not user_id or not role_ids or not isinstance(role_ids, list):
            return make_response(jsonify({"error": "user_id and role_ids (list) are required"}), 400)

        # Check if user exists
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({"error": f"User ID {user_id} does not exist"}), 404)


        # 1. Check for duplicates in one query
        existing_roles = DefUserGrantedRole.query.filter(
            DefUserGrantedRole.user_id == user_id,
            DefUserGrantedRole.role_id.in_(role_ids)
        ).all()
        duplicate_role_ids = [r.role_id for r in existing_roles]

        if duplicate_role_ids:
            return make_response(jsonify({
                "error": "Some roles are already assigned to the user",
                "duplicate_roles": duplicate_role_ids
            }), 409)

        # 2. Fetch all roles in one query to ensure they exist
        roles = DefRoles.query.filter(DefRoles.role_id.in_(role_ids)).all()
        found_role_ids = [r.role_id for r in roles]
        missing_role_ids = list(set(role_ids) - set(found_role_ids))

        if missing_role_ids:
            return make_response(jsonify({
                "error": "Some roles do not exist",
                "missing_roles": missing_role_ids
            }), 404)

        # 3. Create new mappings
        new_mappings = []
        for role in roles:
            new_mapping = DefUserGrantedRole(
                user_id=user_id,
                role_id=role.role_id,
                created_by=get_jwt_identity(),
                creation_date=datetime.utcnow(),
                last_updated_by=get_jwt_identity(),
                last_update_date=datetime.utcnow()
            )
            db.session.add(new_mapping)
            new_mappings.append(new_mapping)

        db.session.commit()

        # Return response with success message
        return make_response(jsonify({
            "message": "Roles assigned successfully",
            "assigned_roles": [m.json() for m in new_mappings]
        }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error creating user-role mappings"
        }), 500)



@flask_app.route('/def_user_granted_roles', methods=['GET'])
@jwt_required()
def get_user_granted_roles():
    try:
        user_id = request.args.get('user_id', type=int)
        role_id = request.args.get('role_id', type=int)

        query = DefUserGrantedRole.query

        # Filter by user_id if provided
        if user_id:
            query = query.filter_by(user_id=user_id)

        # Filter by role_id if provided
        if role_id:
            query = query.filter_by(role_id=role_id)

        results = query.all()

        # If both params given and no record found → return 404
        if user_id and role_id and not results:
            return make_response(jsonify({
                "error": f"No mapping found for user_id={user_id} and role_id={role_id}"
            }), 404)

        return make_response(jsonify([m.json() for m in results]), 200)

    except Exception as e:
        return make_response(jsonify({
            "error": str(e),
            "message": "Error fetching user-role mappings"
        }), 500)





@flask_app.route('/def_user_granted_roles', methods=['PUT'])
@jwt_required()
def update_user_granted_roles():
    try:
        # user_id from query params
        user_id = request.args.get("user_id", type=int)
        if not user_id:
            return make_response(jsonify({"error": "user_id is required"}), 400)

        data = request.json
        role_ids = data.get("role_ids")

        # Validate role_ids
        if not role_ids or not isinstance(role_ids, list):
            return make_response(jsonify({"error": "role_ids (list) is required"}), 400)

        # Check user exists
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({"error": f"User ID {user_id} does not exist"}), 404)

        current_user = get_jwt_identity()
        now = datetime.utcnow()

        # Fetch existing roles
        existing = DefUserGrantedRole.query.filter_by(user_id=user_id).all()
        existing_role_ids = {r.role_id for r in existing}

        incoming_role_ids = set(map(int, role_ids))

        # Determine differences
        to_add = incoming_role_ids - existing_role_ids
        to_remove = existing_role_ids - incoming_role_ids

        # Validate that incoming roles exist
        valid_roles = DefRoles.query.filter(
            DefRoles.role_id.in_(incoming_role_ids)
        ).all()
        found_ids = {r.role_id for r in valid_roles}

        missing = incoming_role_ids - found_ids
        if missing:
            return make_response(jsonify({
                "error": "Some roles do not exist",
                "missing_role_ids": list(missing)
            }), 404)

        # Delete removed roles
        if to_remove:
            DefUserGrantedRole.query.filter(
                DefUserGrantedRole.user_id == user_id,
                DefUserGrantedRole.role_id.in_(to_remove)
            ).delete(synchronize_session=False)

        # Add new roles
        for rid in to_add:
            db.session.add(
                DefUserGrantedRole(
                    user_id=user_id,
                    role_id=rid,
                    created_by=current_user,
                    creation_date=now,
                    last_updated_by=current_user,
                    last_update_date=now
                )
            )

        # Update audit fields for kept roles
        for rid in incoming_role_ids & existing_role_ids:
            mapping = DefUserGrantedRole.query.filter_by(
                user_id=user_id, role_id=rid
            ).first()
            mapping.last_updated_by = current_user
            mapping.last_update_date = now

        db.session.commit()

        return make_response(jsonify({
            "message": "Edited successfully",
            "role_ids": sorted(list(incoming_role_ids))
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error updating user-role mappings"
        }), 500)







@flask_app.route('/def_user_granted_roles', methods=['DELETE'])
@jwt_required()
def delete_user_granted_role():
    try:

        # Extract query parameters
        user_id = request.args.get("user_id", type=int)
        role_id = request.args.get("role_id", type=int)

        if not user_id or not role_id:
            return make_response(jsonify({
                "error": "Missing required query parameters: user_id, role_id"
            }), 400)
        
        # Find the mapping
        mapping = DefUserGrantedRole.query.filter_by(
            user_id=user_id,
            role_id=role_id
        ).first()

        if not mapping:
            return make_response(jsonify({
                "error": f"Mapping for user_id={user_id} and role_id={role_id} not found"
            }), 404)

        # Set who performed the delete (audit tracking)
        mapping.last_updated_by = get_jwt_identity()

        db.session.delete(mapping)
        db.session.commit()

        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error deleting user granted role"
        }), 500)








@flask_app.route('/def_user_granted_privileges', methods=['POST'])
@jwt_required()
def create_user_granted_privileges():
    try:
        data = request.json
        user_id = data.get('user_id')
        privilege_ids = data.get('privilege_ids')

        # Validate input
        if not user_id or not privilege_ids or not isinstance(privilege_ids, list):
            return make_response(jsonify({"error": "user_id and privilege_ids (list) are required"}), 400)

        # Check if user exists
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({"error": f"User ID {user_id} does not exist"}), 404)

        current_user = get_jwt_identity()
        now = datetime.utcnow()

        # 1. Check for duplicates in one query
        existing_privileges = DefUserGrantedPrivilege.query.filter(
            DefUserGrantedPrivilege.user_id == user_id,
            DefUserGrantedPrivilege.privilege_id.in_(privilege_ids)
        ).all()
        duplicate_privilege_ids = [p.privilege_id for p in existing_privileges]

        if duplicate_privilege_ids:
            return make_response(jsonify({
                "error": "Some privileges are already assigned to the user",
                "duplicate_privileges": duplicate_privilege_ids
            }), 409)

        # 2. Fetch all privileges in one query to ensure they exist
        privileges = DefPrivilege.query.filter(DefPrivilege.privilege_id.in_(privilege_ids)).all()
        found_privilege_ids = [p.privilege_id for p in privileges]
        missing_privilege_ids = list(set(privilege_ids) - set(found_privilege_ids))

        if missing_privilege_ids:
            return make_response(jsonify({
                "error": "Some privileges do not exist",
                "missing_privileges": missing_privilege_ids
            }), 404)

        # 3. Create new mappings
        new_mappings = []
        for privilege in privileges:
            new_mapping = DefUserGrantedPrivilege(
                user_id=user_id,
                privilege_id=privilege.privilege_id,
                created_by=current_user,
                creation_date=now,
                last_updated_by=current_user,
                last_update_date=now
            )
            db.session.add(new_mapping)
            new_mappings.append(new_mapping)

        db.session.commit()

        # Return response with success message
        return make_response(jsonify({
            "message": "Privileges assigned successfully",
            "assigned_privileges": [m.json() for m in new_mappings]
        }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error creating user-privilege mappings"
        }), 500)




@flask_app.route('/def_user_granted_privileges', methods=['GET'])
@jwt_required()
def get_user_granted_privileges():
    try:
        user_id = request.args.get("user_id", type=int)
        privilege_id = request.args.get("privilege_id", type=int)

        # If both provided -> single-record lookup
        if user_id is not None and privilege_id is not None:
            record = DefUserGrantedPrivilege.query.filter_by(
                user_id=user_id, privilege_id=privilege_id
            ).first()
            if not record:
                return make_response(jsonify({
                    "error": f"No mapping found for user_id={user_id} and privilege_id={privilege_id}"
                }), 404)
            return make_response(jsonify(record.json()), 200)

        # Otherwise build a list query (may be empty)
        query = DefUserGrantedPrivilege.query

        if user_id is not None:
            query = query.filter_by(user_id=user_id)
        if privilege_id is not None:
            query = query.filter_by(privilege_id=privilege_id)

        records = query.order_by(DefUserGrantedPrivilege.creation_date.desc()).all()
        return make_response(jsonify([r.json() for r in records]), 200)

    except Exception as e:
        return make_response(jsonify({"error": str(e), "message": "Error fetching user-privilege mappings"}), 500)




@flask_app.route('/def_user_granted_privileges', methods=['PUT'])
@jwt_required()
def update_user_granted_privileges():
    try:
        # user_id comes from query param
        user_id = request.args.get("user_id", type=int)
        data = request.json
        privilege_ids = data.get("privilege_ids")

        # Validate input
        if not user_id:
            return make_response(jsonify({"error": "user_id query parameter is required"}), 400)

        if not privilege_ids or not isinstance(privilege_ids, list):
            return make_response(jsonify({
                "error": "privilege_ids (list) is required"
            }), 400)

        # Validate user exists
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({"error": f"User ID {user_id} does not exist"}), 404)

        current_user = get_jwt_identity()
        now = datetime.utcnow()

        # Fetch existing privilege assignments
        existing = DefUserGrantedPrivilege.query.filter_by(
            user_id=user_id
        ).all()
        existing_priv_ids = {p.privilege_id for p in existing}

        incoming_priv_ids = set(privilege_ids)

        # Determine differences
        to_add = incoming_priv_ids - existing_priv_ids
        to_remove = existing_priv_ids - incoming_priv_ids

        # Validate incoming privileges exist
        valid_privileges = DefPrivilege.query.filter(
            DefPrivilege.privilege_id.in_(incoming_priv_ids)
        ).all()
        found_ids = {p.privilege_id for p in valid_privileges}

        missing = incoming_priv_ids - found_ids
        if missing:
            return make_response(jsonify({
                "error": "Some privileges do not exist",
                "missing_privilege_ids": list(missing)
            }), 404)

        # Remove removed privileges
        if to_remove:
            DefUserGrantedPrivilege.query.filter(
                DefUserGrantedPrivilege.user_id == user_id,
                DefUserGrantedPrivilege.privilege_id.in_(to_remove)
            ).delete(synchronize_session=False)

        # Add new privileges
        for pid in to_add:
            db.session.add(
                DefUserGrantedPrivilege(
                    user_id=user_id,
                    privilege_id=pid,
                    created_by=current_user,
                    creation_date=now,
                    last_updated_by=current_user,
                    last_update_date=now
                )
            )

        db.session.commit()

        return make_response(jsonify({
            "message": "Edited successfully",
            "privilege_ids": sorted(list(incoming_priv_ids))
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "error": str(e),
            "message": "Error updating user-privilege mappings"
        }), 500)




@flask_app.route('/def_user_granted_privileges', methods=['DELETE'])
@jwt_required()
def delete_user_granted_privilege():
    try:
        user_id = request.args.get("user_id", type=int)
        privilege_id = request.args.get("privilege_id", type=int)

        # Validate required params
        if user_id is None or privilege_id is None:
            return make_response(jsonify({
                "error": "Query parameters 'user_id' and 'privilege_id' are required"
            }), 400)

        record = DefUserGrantedPrivilege.query.filter_by(
            user_id=user_id,
            privilege_id=privilege_id
        ).first()

        if not record:
            return make_response(jsonify({"error": "User-privilege not found"}), 404)

        db.session.delete(record)
        db.session.commit()

        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)






#!AGGREGATE TABLES AND MATERIALIZED VIEWS


@flask_app.route('/create_aggregate_table', methods=['POST'])
@jwt_required()
def create_aggregate_table():
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({"message": "No JSON data provided"}), 400)

        materialized_view_name = data.get("materialized_view_name")
        schema_name = data.get("schema_name", "public")

        if not materialized_view_name:
            return make_response(jsonify({"message": "Materialized view name is required"}), 400)

        # Construct SQL with both arguments
        sql = text(f"SELECT create_imat('{materialized_view_name}', '{schema_name}')")

        db.session.execute(sql)
        db.session.commit()

        return make_response(jsonify({
            "message": f"Aggregate table created successfully for {materialized_view_name} in schema {schema_name}"
        }), 201)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            "message": "Error creating aggregate table",
            "error": str(e)
        }), 500)







@flask_app.route("/create_mv", methods=["POST"])
def create_mv_endpoint():
    """
    API to create a materialized view with structured payload.
    All helper functions are inside the route.
    """

    # -------------------------
    # HELPER FUNCTIONS
    # -------------------------
    IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

    def validate_ident(name):
        if not isinstance(name, str):
            raise ValueError(f"Identifier must be a string, got: {name!r}")
        if not IDENT_RE.match(name):
            raise ValueError(f"Invalid identifier: {name!r}")

    def build_column_expr(col_def):
        """
        Build SQL for one select item.
        Supports:
        - simple column
        - aggregate functions (COUNT, SUM, AVG, etc.)
        - functions (date_trunc, custom functions)
        - aliasing
        """
        if not isinstance(col_def, dict):
            raise ValueError(f"Select item must be a dict, got: {col_def!r}")

        expr = None

        # 1️⃣ Simple column
        if 'column' in col_def and 'aggregate' not in col_def and 'function' not in col_def:
            expr = col_def['column']

        # 2️⃣ Aggregate function
        elif 'aggregate' in col_def:
            col = col_def.get('column')
            if col is None:
                raise ValueError("Aggregate item must have a 'column' key")
            func = col_def['aggregate'].upper()
            distinct = col_def.get('distinct', False)

            if col == "*":
                expr = f"{func}(*)"
            else:
                expr = f"{func}({('DISTINCT ' if distinct else '')}{col})"

        # 3️⃣ Function call
        elif 'function' in col_def:
            func = col_def['function']
            func_name = func.get('name')
            if not func_name:
                raise ValueError("Function must have a 'name'")
            args = func.get('args', [])
            if not isinstance(args, list):
                raise ValueError("Function 'args' must be a list")
            args_list = []
            for arg in args:
                if isinstance(arg, dict) and 'column' in arg:
                    args_list.append(arg['column'])
                else:
                    args_list.append(str(arg))
            expr = f"{func_name}({', '.join(args_list)})"

        else:
            raise ValueError(f"Unknown select item: {col_def}")

        # 4️⃣ Alias
        alias = col_def.get('alias')
        if alias:
            validate_ident(alias)
            expr += f" AS {alias}"

        if expr is None:
            raise ValueError(f"Failed to build SQL expression from: {col_def}")

        return expr


    def build_select_clause(select_list):
        if not select_list:
            raise ValueError("select list cannot be empty")
        return ", ".join(build_column_expr(item) for item in select_list)

    def build_from_clause(from_obj):
        schema = from_obj.get("schema", "public")
        table = from_obj["table"]
        alias = from_obj.get("alias")
        validate_ident(schema)
        validate_ident(table)
        if alias:
            validate_ident(alias)
        base = f"{schema}.{table}"
        return f"{base} {alias}" if alias else base

    def build_join_clause(joins):
        if not joins:
            return ""
        parts = []
        for j in joins:
            jtype = j.get("type", "INNER").upper()
            schema = j["schema"]
            table = j["table"]
            alias = j.get("alias")
            validate_ident(schema)
            validate_ident(table)
            if alias:
                validate_ident(alias)
            tbl = f"{schema}.{table}" + (f" {alias}" if alias else "")
            conditions = j.get("conditions", [])
            if not conditions:
                raise ValueError(f"Join {tbl} must have at least one condition")
            cond_exprs = []
            for cond in conditions:
                cond_exprs.append(f"{cond['left']} {cond['op']} {cond['right']}")
            cond_str = " AND ".join(cond_exprs)
            parts.append(f"{jtype} JOIN {tbl} ON {cond_str}")
        return " ".join(parts)

    def build_group_by_clause(group_by_list):
        if not group_by_list:
            return ""
        exprs = []
        for item in group_by_list:
            if 'column' in item:
                exprs.append(item['column'])
            elif 'function' in item:
                func = item['function']
                args = func.get('args', [])
                if not args:
                    raise ValueError("Function in group_by must have args")
                args_list = []
                for arg in args:
                    if isinstance(arg, dict) and 'column' in arg:
                        args_list.append(arg['column'])
                    else:
                        args_list.append(str(arg))
                exprs.append(f"{func['name']}({', '.join(args_list)})")
            else:
                raise ValueError(f"Unknown group_by item: {item}")
        return ", ".join(exprs)

    # -------------------------
    # PARSE PAYLOAD
    # -------------------------
    payload = request.get_json()
    if not payload:
        return jsonify({"ok": False, "error": "JSON payload required"}), 400

    mv_name = payload.get("mv_name")
    mv_schema = payload.get("mv_schema", "imat")
    select_list = payload.get("select")
    from_obj = payload.get("from")
    joins = payload.get("joins", [])
    group_by = payload.get("group_by", [])

    # validate identifiers
    try:
        validate_ident(mv_name)
        validate_ident(mv_schema)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    if not select_list or not from_obj:
        return jsonify({"ok": False, "error": "select[] and from{} are required"}), 400

    # -------------------------
    # BUILD SQL
    # -------------------------
    try:
        select_clause = build_select_clause(select_list)
        from_clause = build_from_clause(from_obj)
        join_clause = build_join_clause(joins)
        group_clause = build_group_by_clause(group_by)

        sql = f"SELECT {select_clause} FROM {from_clause}"
        if join_clause:
            sql += " " + join_clause
        if group_clause:
            sql += f" GROUP BY {group_clause}"

    except Exception as e:
        return jsonify({"ok": False, "error": f"Error building SQL: {str(e)}"}), 400

    # -------------------------
    # CREATE MATERIALIZED VIEW
    # -------------------------
    create_mv_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {mv_schema};
    DROP MATERIALIZED VIEW IF EXISTS {mv_schema}.{mv_name} CASCADE;
    CREATE MATERIALIZED VIEW {mv_schema}.{mv_name} AS {sql} WITH NO DATA;
    REFRESH MATERIALIZED VIEW {mv_schema}.{mv_name};
    """

    try:
        conn = db.session.connection()
        conn.execute(text(create_mv_sql))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({
            "error": "Failed creating MV",
            "detail": str(e),
            "sql": sql
        }), 500

    return jsonify({
        "message": "Materialized view created successfully",
        "mv": f"{mv_schema}.{mv_name}",
        "generated_sql": sql
    }), 201





if __name__ == "__main__":
    flask_app.run(debug=True)
