import uuid
import traceback
from datetime import datetime
from src.gold import load_statistics

mode = "reload_date"
run_id = str(uuid.uuid4().hex)
date_of_data = "2026-05-30"  #str(date.today())

try:
    start_date = datetime.now()
    print(start_date)
    load_statistics(mode, date_of_data, run_id)
    end_date = datetime.now()
    print(end_date)
except:
    traceback.print_exc()
    end_date = datetime.now()
    print(end_date)