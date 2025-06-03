import os

"""
Append directories with current directory (relative to caller)
"""
def with_current_dir(*args):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, *args)
