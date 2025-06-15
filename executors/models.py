# tasks.models.py
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB
from .extensions import db
from sqlalchemy import Text, TIMESTAMP
from sqlalchemy.sql import func



class DefTenantEnterpriseSetup(db.Model):
    __tablename__  = 'def_tenant_enterprise_setup'
    __table_args__ = {'schema': 'apps'}
    
    tenant_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    enterprise_name  = db.Column(db.String)
    enterprise_type  = db.Column(db.String)

    def json(self):
        return {
            'tenant_id'       : self.tenant_id,
            'enterprise_name' : self.enterprise_name,
            'enterprise_type' : self.enterprise_type
        }


class DefTenant(db.Model):
    __tablename__  = 'def_tenants'
    __table_args__ = {'schema': 'apps'}
    
    tenant_id   = db.Column(db.Integer, primary_key=True, autoincrement=True)
    tenant_name = db.Column(db.String)

    def json(self):
        return {'tenant_id'  : self.tenant_id,
                'tenant_name': self.tenant_name
           }


class DefTenantEnterpriseSetupV(db.Model):
    __tablename__ = 'def_tenant_enterprise_setup_v'
    __table_args__ = {'schema': 'apps'}

    tenant_id = db.Column(db.Integer, primary_key=True)
    tenant_name = db.Column(db.Text)
    enterprise_name = db.Column(db.Text)
    enterprise_type = db.Column(db.Text)

    def json(self):
        return {
            'tenant_id': self.tenant_id,
            'tenant_name': self.tenant_name,
            'enterprise_name': self.enterprise_name,
            'enterprise_type': self.enterprise_type
        }


class DefUser(db.Model):
    __tablename__  = 'def_users'
    __table_args__ = {'schema': 'apps'}

    user_id         = db.Column(db.Integer, primary_key=True)
    user_name       = db.Column(db.String(40), unique=True, nullable=True)
    user_type       = db.Column(db.String(50))
    email_addresses = db.Column(JSONB, nullable=False )
    created_by      = db.Column(db.Integer, nullable=False)
    created_on      = db.Column(db.String(30))
    last_updated_by = db.Column(db.Integer)
    last_updated_on = db.Column(db.String(50), unique=True, nullable=False)
    tenant_id       = db.Column(db.Integer, db.ForeignKey('apps.def_tenants.tenant_id'), nullable=False) 
    profile_picture = db.Column(
    JSONB,
    default=lambda: {
        "original": "uploads/profiles/default/profile.jpg",
        "thumbnail": "uploads/profiles/default/thumbnail.jpg"
    }
)

    def json(self):
        return {
            'user_id'        : self.user_id,
            'user_name'      : self.user_name,
            'user_type'      : self.user_type,
            'email_addresses': self.email_addresses,
            'created_by'     : self.created_by,
            'created_on'     : self.created_on,
            'last_updated_by': self.last_updated_by,
            'last_updated_on': self.last_updated_on,
            'tenant_id'      : self.tenant_id,
            'profile_picture': self.profile_picture
        }
        

class DefPerson(db.Model):
    __tablename__ = 'def_persons'
    __table_args__ = {'schema': 'apps'}

    user_id     = db.Column(db.Integer, primary_key=True)
    first_name  = db.Column(db.String(40))
    middle_name = db.Column(db.String(30))
    last_name   = db.Column(db.String(30))
    job_title   = db.Column(db.String(50))

    def json(self):
        return {
            'user_id'    : self.user_id,
            'first_name' : self.first_name,
            'middle_name': self.middle_name,
            'last_name'  : self.last_name,
            'job_title'  : self.job_title
        }
    


class DefUserCredential(db.Model):
    __tablename__  = 'def_user_credentials'
    __table_args__ = {'schema': 'apps'}

    user_id  = db.Column(db.Integer, primary_key=True)
    password = db.Column(db.String(50), unique=True, nullable=False)

    def json(self):
        {'user_id' : self.user_id,
         'password': self.password}
        

class DefAccessProfile(db.Model):
    __tablename__ = 'def_access_profiles'
    __table_args__ = {'schema': 'apps'}

    serial_number = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.Integer, db.ForeignKey('apps.def_users.user_id'))
    profile_type = db.Column(db.String(50), nullable=False)
    profile_id = db.Column(db.String(100), nullable=False)
    primary_yn = db.Column(db.CHAR(1), default='N')


    def json(self):
        return {
            'serial_number': self.serial_number,
            'user_id': self.user_id,
            'profile_type': self.profile_type,
            'profile_id': self.profile_id,
            'primary_yn': self.primary_yn
        }
         
        
class DefUsersView(db.Model):
    __tablename__ = 'def_users_v'
    __table_args__ = {'schema': 'apps'}
    
    user_id         = db.Column(db.Integer(), primary_key = True)
    user_name       = db.Column(db.String(50))
    first_name      = db.Column(db.String(30))
    middle_name     = db.Column(db.String(30))
    last_name       = db.Column(db.String(30))
    email_addresses = db.Column(db.String(100))
    job_title       = db.Column(db.String(50))
    created_by      = db.Column(db.Integer())
    created_on      = db.Column(db.String(30))
    last_updated_by = db.Column(db.Integer())
    last_updated_on = db.Column(db.String(50))
    tenant_id       = db.Column(db.Integer())
    profile_picture = db.Column(JSONB)

    def json(self):
        return {
            'user_id'        : self.user_id, 
            'user_name'      : self.user_name,
            'first_name'     : self.first_name,
            'middle_name'    : self.middle_name,
            'last_name'      : self.last_name,
            'email_addresses': self.email_addresses,
            'job_title'      : self.job_title,
            'created_by'     : self.created_by,
            'created_on'     : self.created_on,
            'last_updated_by': self.last_updated_by,
            'last_updated_on': self.last_updated_on,
            'tenant_id'      : self.tenant_id,
            'profile_picture': self.profile_picture
    }
        
        
        
class Message(db.Model):
    __tablename__ = 'messages'
    __table_args__ = {'schema': 'apps'}  

    id            = db.Column(Text, primary_key=True, nullable=False)  
    sender        = db.Column(Text, nullable=False)  
    recivers      = db.Column(JSONB, nullable=False)  
    subject       = db.Column(Text, nullable=True)  
    body          = db.Column(Text, nullable=False)  
    date          = db.Column(TIMESTAMP(timezone=True), nullable=False)  
    status        = db.Column(Text, nullable=False)  
    parentid      = db.Column(Text, nullable=True)  
    involvedusers = db.Column(JSONB, nullable=True)  
    readers       = db.Column(JSONB, nullable=True)  

    # JSON serialization method
    def json(self):
        return {
            'id'            : self.id,
            'sender'        : self.sender,
            'recivers'      : self.recivers,
            'subject'       : self.subject,
            'body'          : self.body,
            'date'          : self.date.isoformat() if self.date else None,
            'status'        : self.status,
            'parentid'      : self.parentid,
            'involvedusers' : self.involvedusers,
            'readers'       : self.readers
        }
    



# class DefAsyncExecutionMethods(db.Model):
#     __tablename__ = 'def_async_execution_methods'

#     execution_method = db.Column(db.String(255), unique=True, nullable=False)  # Unique execution method
#     internal_execution_method = db.Column(db.String(255), primary_key=True) 
#     executor = db.Column(db.String(100))  
#     description = db.Column(db.String(255))  
#     created_by = db.Column(db.Integer)  
#     creation_date = db.Column(db.TIMESTAMP, default=datetime.utcnow)  
#     last_updated_by = db.Column(db.Integer)  
#     last_update_date = db.Column(db.TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)  

#     def json(self):
#         return {
#             "execution_method": self.execution_method,
#             "internal_execution_method": self.internal_execution_method,
#             "executor": self.executor,
#             "description": self.description,
#             "created_by": self.created_by,
#             "creation_date": self.creation_date,
#             "last_updated_by": self.last_updated_by,
#             "last_update_date": self.last_update_date,
#         }
    

class DefAsyncExecutionMethods(db.Model):
    __tablename__ = 'def_async_execution_methods'

    execution_method = db.Column(db.String(255), unique=True, nullable=False)  # Unique execution method
    internal_execution_method = db.Column(db.String(255), primary_key=True) 
    executor = db.Column(db.String(100))  
    description = db.Column(db.String(255))
    created_by = db.Column(db.Integer)
    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    last_updated_by = db.Column(db.Integer)
    last_update_date = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  
    def json(self):
        return {
            "execution_method": self.execution_method,
            "internal_execution_method": self.internal_execution_method,
            "executor": self.executor,
            "description": self.description,
            "created_by": self.created_by,
            "creation_date": self.creation_date,
            "last_updated_by": self.last_updated_by,
            "last_update_date": self.last_update_date
        }

 
class DefAsyncTask(db.Model):
    __tablename__ = 'def_async_tasks'

    def_task_id = db.Column(db.Integer, primary_key=True)  # Auto-incrementing primary key
    user_task_name = db.Column(db.String(255), nullable=False)
    task_name = db.Column(db.String(255), nullable=False, unique=True)  # Task name (required)
    internal_execution_method = db.Column(db.String(255), primary_key=True)
    execution_method = db.Column(db.String(100))  # Execution method (optional)
    executor         = db.Column(db.String(100), nullable=False)
    script_name      = db.Column(db.String(100))  # Script name (optional)
    script_path      = db.Column(db.String(100))
    description      = db.Column(db.String(255))  # Description (optional)
    cancelled_yn     = db.Column(db.String(1), default='N')  # Default 'N'
    srs              = db.Column(db.String(1), default='N')  # Default 'N'
    sf               = db.Column(db.String(1), default='N')  # Default 'N'
    created_by       = db.Column(db.Integer)  # User who created the record (optional)
    creation_date    = db.Column(db.TIMESTAMP, default=datetime.utcnow)  # Timestamp of creation
    last_updated_by  = db.Column(db.Integer)  # User who last updated the record (optional)
    last_update_date = db.Column(db.TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)  # Timestamp of last update

    def json(self):
        return {
            "def_task_id": self.def_task_id,
            "user_task_name": self.user_task_name,
            "task_name": self.task_name,
            "internal_execution_method": self.internal_execution_method,
            "execution_method": self.execution_method,
            "executor": self.executor,
            "script_name": self.script_name,
            "script_path" : self.script_path,
            "description": self.description,
            "cancelled_yn": self.cancelled_yn,
            "srs": self.srs,
            "sf": self.sf,
            "created_by": self.created_by,
            "creation_date": self.creation_date,
            "last_updated_by": self.last_updated_by,
            "last_update_date": self.last_update_date,
        }
        
class DefAsyncTaskParam(db.Model):
    __tablename__ = 'def_async_task_params'

    def_param_id = db.Column(db.Integer, primary_key=True)  # Auto-incrementing primary key
    task_name = db.Column(db.String(255), nullable=False)  # Task name (required)
    #seq = db.Column(db.Integer, nullable=False)  # Sequence/order (required)
    parameter_name = db.Column(db.String(150))  # Parameter name (optional)
    data_type = db.Column(db.String(100))  # Data type (optional)
    description = db.Column(db.String(250))  # Description (optional)
    created_by = db.Column(db.Integer)  # User who created the record (optional)
    creation_date = db.Column(db.TIMESTAMP, default=datetime.utcnow)  # Timestamp of creation
    last_updated_by = db.Column(db.Integer)  # User who last updated the record (optional)
    last_update_date = db.Column(db.TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)  # Timestamp of last update

    def json(self):
        return {
            "def_param_id": self.def_param_id,
            "task_name": self.task_name,
            "parameter_name": self.parameter_name,
            "data_type": self.data_type,
            "description": self.description,
            "created_by": self.created_by,
            "creation_date": self.creation_date,
            "last_updated_by": self.last_updated_by,
            "last_update_date": self.last_update_date,
        }


class DefAsyncTaskSchedule(db.Model):
    __tablename__ = 'def_async_task_schedules'

    def_task_sche_id = db.Column(db.Integer, primary_key=True)  # Auto-incrementing primary key
    user_schedule_name = db.Column(db.String(255), nullable=False)  # Schedule name (required)
    redbeat_schedule_name = db.Column(db.String(255), nullable=False)
    task_name = db.Column(db.String(255), nullable=False)  # Task name (required)
    args = db.Column(JSONB)  # Arguments for the task (optional)
    kwargs = db.Column(JSONB)  # Keyword arguments for the task (optional)
    schedule = db.Column(JSONB)  # Schedule info (optional)
    cancelled_yn = db.Column(db.String(1), default='N')  # Default 'N'
    created_by = db.Column(db.Integer)  # User who created the record (optional)
    creation_date = db.Column(db.TIMESTAMP, default=datetime.utcnow)  # Timestamp of creation
    last_updated_by = db.Column(db.Integer)  # User who last updated the record (optional)
    last_update_date = db.Column(db.TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)  # Timestamp of last update

    def json(self):
        return {
            "def_task_sche_id": self.def_task_sche_id,
            "user_schedule_name": self.user_schedule_name,
            "redbeat_schedule_name": self.redbeat_schedule_name,
            "task_name": self.task_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "schedule": self.schedule,
            "cancelled_yn": self.cancelled_yn,
            "created_by": self.created_by,
            "creation_date": self.creation_date,
            "last_updated_by": self.last_updated_by,
            "last_update_date": self.last_update_date,
        }
    


class DefAsyncTaskScheduleNew(db.Model):
    __tablename__ = 'def_async_task_schedules'
    __table_args__ = {'extend_existing': True}  # Allow redefinition

    def_task_sche_id = db.Column(db.Integer, primary_key=True)  # Auto-incrementing primary key
    user_schedule_name = db.Column(db.String(255), nullable=False)  # Schedule name (required)
    redbeat_schedule_name = db.Column(db.String(255), nullable=False)
    task_name = db.Column(db.String(255), nullable=False)  # Task name (required)
    args = db.Column(JSONB)  # Arguments for the task (optional)
    kwargs = db.Column(JSONB)  # Keyword arguments for the task (optional)
    schedule_type = db.Column(db.String(50))
    parameters = db.Column(db.JSON)
    schedule = db.Column(JSONB)  # Schedule info (optional)
    # ready_for_redbeat = db.Column(db.String(1))
    cancelled_yn = db.Column(db.String(1), default='N')  # Default 'N'
    created_by = db.Column(db.Integer)  # User who created the record (optional)
    creation_date = db.Column(db.TIMESTAMP, default=datetime.utcnow)  # Timestamp of creation
    last_updated_by = db.Column(db.Integer)  # User who last updated the record (optional)
    last_update_date = db.Column(db.TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)  # Timestamp of last update

    def json(self):
        return {
            "def_task_sche_id": self.def_task_sche_id,
            "user_schedule_name": self.user_schedule_name,
            "redbeat_schedule_name": self.redbeat_schedule_name,
            "task_name": self.task_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "parameters": self.parameters,
            "schedule_type": self.schedule_type,
            "schedule": self.schedule,
            # "ready_for_redbeat": self.ready_for_redbeat,
            "cancelled_yn": self.cancelled_yn,
            "created_by": self.created_by,
            "creation_date": self.creation_date,
            "last_updated_by": self.last_updated_by,
            "last_update_date": self.last_update_date,
        }




class DefAsyncTaskRequest(db.Model):
    __tablename__ = 'def_async_task_requests'

    request_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    task_id = db.Column(db.String(200), nullable=False, unique=True)
    status = db.Column(db.String(50))
    user_task_name = db.Column(db.String(200))
    task_name = db.Column(db.String(200))
    executor = db.Column(db.String(200))
    user_schedule_name = db.Column(db.String(200))
    redbeat_schedule_name = db.Column(db.String(200))
    schedule_type = db.Column(db.String(50))
    schedule = db.Column(db.JSON)
    args = db.Column(db.JSON)
    kwargs = db.Column(db.JSON)
    parameters = db.Column(db.JSON)
    result = db.Column(db.JSON)
    timestamp = db.Column(db.TIMESTAMP, default=datetime.utcnow)
    created_by = db.Column(db.Integer)  
    creation_date = db.Column(db.DateTime, default=datetime.utcnow) 
    last_updated_by = db.Column(db.Integer)  
    last_update_date = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow) 
    

    def json(self):
        return {
            "request_id": self.request_id,
            "task_id": self.task_id,
            "status": self.status,
            "user_task_name": self.user_task_name,
            "task_name": self.task_name,
            "executor": self.executor,
            "user_schedule_name": self.user_schedule_name,
            "redbeat_schedule_name": self.redbeat_schedule_name,
            "schedule_type": self.schedule_type,
            "schedule": self.schedule,
            "args": self.args,
            "kwargs": self.kwargs,
            "parameters": self.parameters,
            "result": self.result,
            "timestamp": self.timestamp,
            "created_by": self.created_by,
            "creation_date": self.creation_date,
            "last_updated_by": self.last_updated_by,
            "last_update_date": self.last_update_date,
        }

    

class DefAsyncTaskSchedulesV(db.Model):
    __tablename__ = 'def_async_task_schedules_v'  # View name
    
    def_task_sche_id = db.Column(db.Integer, primary_key=True)
    user_schedule_name = db.Column(db.String(255), nullable=False)
    redbeat_schedule_name = db.Column(db.String(255), nullable=False)
    user_task_name = db.Column(db.String(200))
    task_name = db.Column(db.String(255), nullable=False)
    args = db.Column(JSONB)  # Arguments for the task
    kwargs = db.Column(JSONB)  # Keyword arguments for the task
    parameters = db.Column(db.JSON)
    schedule_type = db.Column(db.String(255))
    schedule = db.Column(db.Integer)  # Schedule interval
    # ready_for_redbeat = db.Column(db.String(1), default='N')
    cancelled_yn = db.Column(db.String(1), default='N')  # 'Y' or 'N' for cancellation status
    created_by = db.Column(db.Integer)  # User who created the task
    creation_date = db.Column(db.DateTime, default=datetime.utcnow)  # Timestamp of creation
    last_updated_by = db.Column(db.Integer)  # User who last updated the task
    last_update_date = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Timestamp of last update
    
    # JSON method to return data as a dictionary
    def json(self):
        return {
            "def_task_sche_id": self.def_task_sche_id,
            "user_schedule_name": self.user_schedule_name,
            "redbeat_schedule_name": self.redbeat_schedule_name,
            "user_task_name": self.user_task_name,
            "task_name": self.task_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "parameters": self.parameters,
            "schedule_type": self.schedule_type,
            "schedule": self.schedule,
            # "ready_for_redbeat": self.ready_for_redbeat,
            "cancelled_yn": self.cancelled_yn,
            "created_by": self.created_by,
            "creation_date": self.creation_date,
            "last_updated_by": self.last_updated_by,
            "last_update_date": self.last_update_date
        }
    

class DefAccessModel(db.Model):
    __tablename__ = 'def_access_models'
    __table_args__ = {'schema': 'apps'}

    def_access_model_id = db.Column(db.Integer, primary_key=True)  
    model_name         = db.Column(db.Text)                       
    description        = db.Column(db.Text)                       
    type               = db.Column(db.Text)                       
    run_status         = db.Column(db.Text)                       
    state              = db.Column(db.Text)                       
    last_run_date      = db.Column(db.DateTime, default=datetime.utcnow)                       
    created_by         = db.Column(db.Text)                       
    last_updated_by    = db.Column(db.Text)                       
    last_updated_date  = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)                       
    revision           = db.Column(db.Integer)                    
    revision_date      = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    datasource_name = db.Column(db.Text, db.ForeignKey('apps.def_data_sources.datasource_name', name='datasource_name'), nullable=True)                       


    # logics = db.relationship("DefAccessModelLogic", back_populates="model")

    def json(self):
        return {
            "def_access_model_id": self.def_access_model_id,
            "model_name": self.model_name,
            "description": self.description,
            "type": self.type,
            "run_status": self.run_status,
            "state": self.state,
            "last_run_date": self.last_run_date.isoformat() if self.last_run_date else None,
            "created_by": self.created_by,
            "last_updated_by": self.last_updated_by,
            "last_updated_date": self.last_updated_date.isoformat() if self.last_updated_date else None,
            "revision": self.revision,
            "revision_date": self.revision_date.isoformat() if self.revision_date else None,
            "datasource_name": self.datasource_name
        }

class DefAccessModelLogic(db.Model):
    __tablename__ = 'def_access_model_logics'
    __table_args__ = {'schema': 'apps'}

    def_access_model_logic_id = db.Column(db.Integer, primary_key=True) 
    def_access_model_id       = db.Column(db.Integer, db.ForeignKey('apps.def_access_models.def_access_model_id'), nullable=False)  # Foreign key to def_access_models
    filter                    = db.Column(db.Text)                       
    object                    = db.Column(db.Text)                       
    attribute                 = db.Column(db.Text)                       
    condition                 = db.Column(db.Text)                       
    value                     = db.Column(db.Text)                       

    
    # model = db.relationship("DefAccessModel", back_populates="logics")
    # attributes = db.relationship("DefAccessModelLogicAttribute", back_populates="logic")

    def json(self):
        return {
            "def_access_model_logic_id": self.def_access_model_logic_id,
            "def_access_model_id": self.def_access_model_id,
            "filter": self.filter,
            "object": self.object,
            "attribute": self.attribute,
            "condition": self.condition,
            "value": self.value
        }

class DefAccessModelLogicAttribute(db.Model):
    __tablename__ = 'def_access_model_logic_attributes'
    __table_args__ = {'schema': 'apps'}

    id                        = db.Column(db.Integer, primary_key=True)  
    def_access_model_logic_id = db.Column(db.Integer, db.ForeignKey('apps.def_access_model_logics.def_access_model_logic_id'), nullable=False)  # Foreign key to def_access_model_logics
    widget_position           = db.Column(db.Integer)                    
    widget_state              = db.Column(db.Integer)                    

    
    # logic = db.relationship("DefAccessModelLogic", back_populates="attributes")

    def json(self):
        return {
            "id": self.id,
            "def_access_model_logic_id": self.def_access_model_logic_id,
            "widget_position": self.widget_position,
            "widget_state": self.widget_state
        }
    

class DefGlobalCondition(db.Model):
    __tablename__  = 'def_global_conditions'
    __table_args__ = {'schema': 'apps'}

    def_global_condition_id = db.Column(db.Integer, primary_key=True)
    name        = db.Column(db.Text)
    datasource  = db.Column(db.Text)
    description = db.Column(db.Text)
    status      = db.Column(db.Text)

    def json(self):
        return {
            'def_global_condition_id': self.def_global_condition_id,
            'name'       : self.name,
            'datasource' : self.datasource,
            'description': self.description,
            'status'     : self.status
        }
    
class DefGlobalConditionLogic(db.Model):
    __tablename__  = 'def_global_condition_logics'
    __table_args__ = {'schema': 'apps'}

    def_global_condition_logic_id = db.Column(db.Integer, primary_key=True)
    def_global_condition_id       = db.Column(db.Integer, db.ForeignKey('apps.def_global_conditions.def_global_condition_id'), nullable=False)
    object     = db.Column(db.Text)
    attribute  = db.Column(db.Text)
    condition  = db.Column(db.Text)
    value      = db.Column(db.Text)

    def json(self):
        return {
            'def_global_condition_logic_id': self.def_global_condition_logic_id,
            'def_global_condition_id'     : self.def_global_condition_id,
            'object'     : self.object,
            'attribute'  : self.attribute,
            'condition'  : self.condition,
            'value'      : self.value
        }
    
class DefGlobalConditionLogicAttribute(db.Model):

    __tablename__  = 'def_global_condition_logic_attributes'
    __table_args__ = {'schema': 'apps'}

    id = db.Column(db.Integer, primary_key=True)
    def_global_condition_logic_id = db.Column(db.Integer, db.ForeignKey('apps.def_global_condition_logics.def_global_condition_logic_id'), nullable=False)
    widget_position = db.Column(db.Integer)
    widget_state    = db.Column(db.Integer)

    def json(self):
        return {
            'id': self.id,
            'def_global_condition_logic_id' : self.def_global_condition_logic_id,
            'widget_position' : self.widget_position,
            'widget_state'    : self.widget_state
        }
    

class DefDataSource(db.Model):
    __tablename__ = 'def_data_sources'
    __table_args__ = {'schema': 'apps'}

    def_data_source_id                     = db.Column(db.Integer, primary_key=True, autoincrement=True)
    datasource_name                       = db.Column(db.String(50))
    description                           = db.Column(db.String(250))
    application_type                      = db.Column(db.String(50))
    application_type_version              = db.Column(db.String(50))
    last_access_synchronization_date      = db.Column(db.DateTime)
    last_access_synchronization_status    = db.Column(db.String(50))
    last_transaction_synchronization_date = db.Column(db.DateTime)
    last_transaction_synchronization_status = db.Column(db.String(50))
    default_datasource                    = db.Column(db.String(50))
    created_by                            = db.Column(db.String(50))
    created_on                            = db.Column(db.DateTime, default=datetime.utcnow)
    last_updated_by                       = db.Column(db.String(50))
    last_updated_on                       = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def json(self):
        return {
            "def_data_source_id": self.def_data_source_id,
            "datasource_name": self.datasource_name,
            "description": self.description,
            "application_type": self.application_type,
            "application_type_version": self.application_type_version,
            "last_access_synchronization_date": self.last_access_synchronization_date,
            "last_access_synchronization_status": self.last_access_synchronization_status,
            "last_transaction_synchronization_date": self.last_transaction_synchronization_date,
            "last_transaction_synchronization_status": self.last_transaction_synchronization_status,
            "default_datasource": self.default_datasource,
            "created_by": self.created_by,
            "created_on": self.created_on,
            "last_updated_by": self.last_updated_by,
            "last_updated_on": self.last_updated_on
        }

class DefAccessPointElement(db.Model):
    __tablename__ = 'def_access_point_elements'
    __table_args__ = {'schema': 'apps'}

    def_access_point_id  = db.Column(db.Integer, primary_key=True, autoincrement=True)
    def_data_source_id   = db.Column(db.Integer, db.ForeignKey('apps.def_data_sources.def_data_source_id'), nullable=False)
    element_name         = db.Column(db.String(150))
    description          = db.Column(db.String(250))
    platform             = db.Column(db.String(50))
    element_type         = db.Column(db.String(50))
    access_control       = db.Column(db.String(10))
    change_control       = db.Column(db.String(10))
    audit                = db.Column(db.String(50))
    created_by           = db.Column(db.String(50))  
    created_on           = db.Column(db.DateTime, default=datetime.utcnow)
    last_updated_by      = db.Column(db.String(50)) 
    last_updated_on      = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def json(self):
        return {
            "def_access_point_id": self.def_access_point_id,
            "def_data_source_id": self.def_data_source_id,
            "element_name": self.element_name,
            "description": self.description,
            "platform": self.platform,
            "element_type": self.element_type,
            "access_control": self.access_control,
            "change_control": self.change_control,
            "audit": self.audit,
            "created_by": self.created_by,
            "created_on": self.created_on,
            "last_updated_by": self.last_updated_by,
            "last_updated_on": self.last_updated_on
        }

class DefAccessEntitlement(db.Model):
    __tablename__ = 'def_access_entitlements'
    __table_args__ = {'schema': 'apps'}

    def_entitlement_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    entitlement_name = db.Column(db.String(150), nullable=False)  
    description = db.Column(db.String(250))                        
    comments = db.Column(db.String(200))                           
    status = db.Column(db.String(50), nullable=False)              
    effective_date = db.Column(db.Date, nullable=False)           
    revision = db.Column(db.String(10))                            
    revision_date = db.Column(db.Date)                            
    created_by = db.Column(db.String(50))                          
    created_on = db.Column(db.DateTime, default=datetime.utcnow)   
    last_updated_by = db.Column(db.String(50))                     
    last_updated_on = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def json(self):
        return {
            "def_entitlement_id": self.def_entitlement_id,
            "entitlement_name": self.entitlement_name,
            "description": self.description,
            "comments": self.comments,
            "status": self.status,
            "effective_date": self.effective_date,
            "revision": self.revision,
            "revision_date": self.revision_date,
            "created_by": self.created_by,
            "created_on": self.created_on,
            "last_updated_by": self.last_updated_by,
            "last_updated_on": self.last_updated_on
        }

class DefControl(db.Model):
    __tablename__ = 'def_controls'
    __table_args__ = {'schema': 'apps'}

    def_control_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    control_name         = db.Column(db.Text)
    description          = db.Column(db.Text)
    pending_results_count = db.Column(db.Integer)
    control_type         = db.Column(db.Text)
    priority             = db.Column(db.Integer)
    datasources          = db.Column(db.Text)
    last_run_date     = db.Column(db.Text)
    last_updated_date = db.Column(db.Text)
    status               = db.Column(db.Text)
    state                = db.Column(db.Text)
    result_investigator  = db.Column(db.Text)
    authorized_data      = db.Column(db.Text)
    revision             = db.Column(db.Integer)
    revision_date        = db.Column(db.Text)
    created_by           = db.Column(db.Text)
    created_date         = db.Column(db.Text)

    def json(self):
        return {
            "def_control_id": self.def_control_id,
            "control_name": self.control_name,
            "description": self.description,
            "pending_results_count": self.pending_results_count,
            "control_type": self.control_type,
            "priority": self.priority,
            "datasources": self.datasources,
            "last_run_date": self.last_run_date,
            "last_updated_date": self.last_updated_date,
            "status": self.status,
            "state": self.state,
            "result_investigator": self.result_investigator,
            "authorized_data": self.authorized_data,
            "revision": self.revision,
            "revision_date": self.revision_date,
            "created_by": self.created_by,
            "created_date": self.created_date
        }


