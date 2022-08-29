# -*- coding: utf-8 -*-
from contextlib import contextmanager
import time
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')


@contextmanager
def timeit_function(func_name):
    """
    A contextmanager to measure time consuming of assigned function

    :param func_name: description of name of function
    """

    print(f"Function: {func_name} start running")

    start = time.time()
    yield
    duration = (time.time() - start)

    print(f"Function: {func_name} took {duration:0f}")
