"""
Module to handle the standard logging output
a log entry is interesting if it indicates from which module it was executed
generally we should have two possibilities :
- generate a section
- generate a log ery
"""
from pathlib import Path

def write_log_section(section_name: str):
    print('*** ' + section_name + ' ***')

def write_log_entry(context: str, event: str):
    p = Path(context)
    print(p.stem + ' | ' + event)

def write_line():
    print('')
