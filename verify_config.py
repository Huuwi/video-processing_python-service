
import sys
import os
sys.stdout.reconfigure(encoding='utf-8')
sys.path.append(os.getcwd())
try:
    from app.core.config import settings
    with open("verify_output.txt", "w") as f:
        f.write(f"MAX: {settings.MAX_DURATION_MINUTES}\n")
        f.write(f"CHUNK: {settings.CHUNK_DURATION_MINUTES}\n")
except Exception as e:
    with open("verify_output.txt", "w") as f:
        f.write(f"Error: {e}\n")
