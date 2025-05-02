# tasks.models.py
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB
from .extensions import db
from sqlalchemy import Text
from sqlalchemy import Text, TIMESTAMP



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
    email_addresses = db.Column(db.JSON, nullable=False )
    created_by      = db.Column(db.Integer, nullable=False)
    created_on      = db.Column(db.String(30))
    last_updated_by = db.Column(db.Integer)
    last_updated_on = db.Column(db.String(50), unique=True, nullable=False)
    tenant_id       = db.Column(db.Integer, db.ForeignKey('apps.def_tenants.tenant_id'), nullable=False) 

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
            'tenant_id'      : self.tenant_id
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
            'tenant_id'      : self.tenant_id
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
    def json(self):
        return {
            "execution_method": self.execution_method,
            "internal_execution_method": self.internal_execution_method,
            "executor": self.executor,
            "description": self.description
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
    ready_for_redbeat = db.Column(db.String(1))
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
            "ready_for_redbeat": self.ready_for_redbeat,
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
    ready_for_redbeat = db.Column(db.String(1), default='N')
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
            "ready_for_redbeat": self.ready_for_redbeat,
            "cancelled_yn": self.cancelled_yn,
            "created_by": self.created_by,
            "creation_date": self.creation_date,
            "last_updated_by": self.last_updated_by,
            "last_update_date": self.last_update_date
        }
    

