from sqlalchemy import create_engine, text
import os

db_connection_string = os.environ.get('DB_CONNECTION_STRING')

if not db_connection_string:
    raise ValueError("DB_CONNECTION_STRING environment variable is not set.")

print("Connecting to database with connection string:", db_connection_string)

engine = create_engine(
    db_connection_string,
    connect_args={
        "sslmode": "require"  # For SSL connections
    }
)

def load_jobs_from_db():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM jobs"))
        jobs = [dict(row) for row in result]
        return jobs

def load_job_from_db(id):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": id})
        rows = result.fetchall()
        if len(rows) == 0:
            return None
        return dict(rows[0])

def add_application_to_db(job_id, data):
    with engine.connect() as conn:
        query = text("""
            INSERT INTO applications (job_id, full_name, email, linkedin_url, education, work_experience, resume_url) 
            VALUES (:job_id, :full_name, :email, :linkedin_url, :education, :work_experience, :resume_url)
        """)
        conn.execute(query, {
            "job_id": job_id,
            "full_name": data['full_name'],
            "email": data['email'],
            "linkedin_url": data['linkedin_url'],
            "education": data['education'],
            "work_experience": data['work_experience'],
            "resume_url": data['resume_url']
        })
