# tasks.models.py
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB

from .extensions import db


# class ArmAsyncExecutionMethods(db.Model):
#     __tablename__ = 'arm_async_execution_methods'

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
    

class ArmAsyncExecutionMethods(db.Model):
    __tablename__ = 'arm_async_execution_methods'

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

 
class ArmAsyncTask(db.Model):
    __tablename__ = 'arm_async_tasks'

    arm_task_id = db.Column(db.Integer, primary_key=True)  # Auto-incrementing primary key
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
            "arm_task_id": self.arm_task_id,
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
        
class ArmAsyncTaskParam(db.Model):
    __tablename__ = 'arm_async_task_params'

    arm_param_id = db.Column(db.Integer, primary_key=True)  # Auto-incrementing primary key
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
            "arm_param_id": self.arm_param_id,
            "task_name": self.task_name,
            "parameter_name": self.parameter_name,
            "data_type": self.data_type,
            "description": self.description,
            "created_by": self.created_by,
            "creation_date": self.creation_date,
            "last_updated_by": self.last_updated_by,
            "last_update_date": self.last_update_date,
        }


class ArmAsyncTaskSchedule(db.Model):
    __tablename__ = 'arm_async_task_schedules'

    arm_task_sche_id = db.Column(db.Integer, primary_key=True)  # Auto-incrementing primary key
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
            "arm_task_sche_id": self.arm_task_sche_id,
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
    


class ArmAsyncTaskScheduleNew(db.Model):
    __tablename__ = 'arm_async_task_schedules'
    __table_args__ = {'extend_existing': True}  # Allow redefinition

    arm_task_sche_id = db.Column(db.Integer, primary_key=True)  # Auto-incrementing primary key
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
            "arm_task_sche_id": self.arm_task_sche_id,
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




class ArmAsyncTaskRequest(db.Model):
    __tablename__ = 'arm_async_task_requests'

    request_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    task_id = db.Column(db.String(200), nullable=False, unique=True)
    status = db.Column(db.String(50))
    user_task_name = db.Column(db.String(200))
    task_name = db.Column(db.String(200))
    executor = db.Column(db.String(200))
    user_schedule_name = db.Column(db.String(200))
    redbeat_schedule_name = db.Column(db.String(200))
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

    

class ArmAsyncTaskSchedulesV(db.Model):
    __tablename__ = 'arm_async_task_schedules_v'  # View name
    
    arm_task_sche_id = db.Column(db.Integer, primary_key=True)
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
            "arm_task_sche_id": self.arm_task_sche_id,
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
    


