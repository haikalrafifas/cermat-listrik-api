import os

"""
Append directories with base directory
"""
def with_base_dir(**kwargs):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, *[v for v in kwargs.values()])
