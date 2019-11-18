import sys, os
import datetime, time

file_now_str = lambda: datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
now = lambda: datetime.datetime.now()
now_str = lambda: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
to_list = lambda x: x if isinstance(x, list) else [x]

def get_duration(ts):
  duration = time.time() - ts
  tmin, tsec = int(duration/60), int(duration - int(duration/60)*60)
  return f'{tmin} min {tsec} sec'

def log(t): sys.stdout.write(f'{now_str()} -- {t}\n'); sys.stdout.flush()
