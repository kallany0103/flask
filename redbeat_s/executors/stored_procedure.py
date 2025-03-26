import os
import psycopg2
from celery import shared_task
import logging

logging.basicConfig(level=logging.INFO)

db_url = os.getenv("database_url_01")

@shared_task(bind=True)
def execute(self, *args, **kwargs):
    stored_procedure_name = args[0] if len(args) > 0 else None
    user_task_name = args[1] if len(args) > 1 else None
    task_name = args[2] if len(args) > 2 else None
    user_schedule_name = args[3] if len(args) > 3 else None
    redbeat_schedule_name = args[4] if len(args) > 4 else None
    schedule = args[5] if len(args) > 5 else None
    params = kwargs

    conn = None
    cursor = None

    try:
        logging.info("Attempting to connect to the database...")
        conn = psycopg2.connect(db_url)
        logging.info("Database connection successful.")

        if conn is None:
            logging.error("Database connection object is None.")
            return {"error": "Failed to establish database connection."}

        cursor = conn.cursor()

        # Add a placeholder for the OUT parameter
        call_query = f"CALL {stored_procedure_name}(%s);"
        out_param = None  # This will hold the output value

        cursor.execute(call_query, (out_param,))  # Execute the stored procedure
        output = cursor.fetchone()  # Fetch the output

        conn.commit()

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
            "result": output[0] if output else None,  # Extracting the first column value
            "message": "Stored procedure executed successfully!"
        }

    except psycopg2.Error as db_err:
        logging.error(f"psycopg2 error: {db_err}", exc_info=True)
        return {"error": f"Stored procedure execution failed: {str(db_err)}"}
    except Exception as e:
        logging.exception("An unexpected error occurred:")
        return {"error": f"Stored procedure execution failed: {str(e)}"}

    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                logging.error(f"Failed to close cursor: {e}", exc_info=True)

        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                logging.error(f"Failed to close connection: {e}", exc_info=True)
