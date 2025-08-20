import json
import sys
import io
import os
from celery import shared_task, states
from celery.exceptions import Ignore

script_path = os.getenv("SCRIPT_PATH_01")

@shared_task(bind=True)
def execute(self, *args, **kwargs):
    script_name = args[0] if len(args) > 0 else None
    user_task_name = args[1] if len(args) > 1 else None
    task_name = args[2] if len(args) > 2 else None
    user_schedule_name = args[3] if len(args) > 3 else None
    redbeat_schedule_name = args[4] if len(args) > 4 else None
    schedule_type = args[5] if len(args) > 5 else None
    schedule = args[6] if len(args) > 6 else None
    
    
    params = kwargs
    full_script_path = os.path.join(script_path, script_name) if script_name else None

    original_stdout = sys.stdout
    sys.stdout = io.StringIO()

    try:
        if not script_name or not os.path.exists(full_script_path):
            raise FileNotFoundError(f"Script not found: {full_script_path}")

        with open(full_script_path, 'r') as file:
            script_content = file.read()

        exec_globals = {"__builtins__": __builtins__}
        exec_globals.update(params)

        exec(script_content, exec_globals)

        raw_output = sys.stdout.getvalue().strip()

        try:
            output = json.loads(raw_output)
        except json.JSONDecodeError:
            output = {"output": raw_output}

        return {
            "user_task_name": user_task_name,
            "task_name": task_name,
            "executor": self.name,
            "user_schedule_name": user_schedule_name,
            "redbeat_schedule_name": redbeat_schedule_name,
            "schedule_type": schedule_type,
            "schedule": schedule,
            "args": args,
            "kwargs": params,
            "parameters": params,
            "result": output,
            "message": "Script executed successfully"
        }
    
    except Exception as exc:
        #Update state to FAILURE and attach exception metadata
        self.update_state(
            state=states.FAILURE,
            meta={
                "exc_type": type(exc).__name__,
                "exc_message": str(exc)
            }
        )
        # Raise Ignore so Celery marks task as FAILURE
        raise Ignore()

    # except Exception as e:
    #     failure_data = {
    #         # "status": "FAILURE",
    #         "user_task_name": user_task_name,
    #         "task_name": task_name,
    #         "executor": self.name,
    #         "user_schedule_name": user_schedule_name,
    #         "redbeat_schedule_name": redbeat_schedule_name,
    #         "schedule_type": schedule_type,
    #         "schedule": schedule,
    #         "args": args,
    #         "kwargs": params,
    #         "parameters": params,
    #         "error": str(e),
    #         "message": "Script execution failed"
    #     }

    #     # Store failure info in result backend
    #     self.update_state(
    #         state="FAILURE",
    #         meta=failure_data
    #     )

    #     # Raise so Celery still marks as FAILURE
    #     raise Exception(json.dumps(failure_data))

    finally:
        sys.stdout = original_stdout
