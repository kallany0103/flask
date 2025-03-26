from celery import shared_task
import subprocess
import sys
import io
import os

script_path = os.getenv("script_path_01")
    
@shared_task(bind=True)
def execute(self, *args, **kwargs):
    # Default values as None (null)
    #full_script_path = "E:\\python_scripts\\get_employee_name.py"
    script_name = args[0] if len(args) > 0 else None
    user_task_name = args[1] if len(args) > 1 else None
    task_name = args[2] if len(args) > 2 else None
    user_schedule_name = args[3] if len(args) > 3 else None
    redbeat_schedule_name = args[4] if len(args) > 4 else None
    schedule = args[5] if len(args) > 5 else None
   

    params = kwargs  # Use kwargs as script parameters
    full_script_path = os.path.join(script_path, script_name)
    try:
        #  Capture script output
        original_stdout = sys.stdout
        sys.stdout = io.StringIO()
        #  Read & Execute the script
        with open(full_script_path, 'r') as file:
            script_content = file.read()

        exec_globals = {"__builtins__": __builtins__}  # Safe execution context
        exec_globals.update(params)  # Inject parameters

        exec(script_content, exec_globals)

        #  Get script output
        output = sys.stdout.getvalue().strip()

        return {
            "user_task_name": user_task_name,
            "task_name": task_name,
            "executor": self.name,
            "user_schedule_name": user_schedule_name, 
            "redbeat_schedule_name": redbeat_schedule_name, 
            "schedule": schedule,
            "args": args,
            "kwargs": params,
            "parameters": params,
            "result": output,
            "message": "Script executed successfully!"
        }

    except Exception as e:
        return {"error": f"Script execution failed: {str(e)}"}

    finally:
        # Restore stdout safely
        sys.stdout = original_stdout
