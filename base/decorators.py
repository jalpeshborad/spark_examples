# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

from time import time


def time_taken(func, *args, **kwargs):
    def wrap():
        t1 = time()
        func(*args, **kwargs)
        print(f"Time taken to execute: {time() - t1} seconds")
    return wrap


def catch_keyboard_interrupt(func, *args, **kwargs):
    def wrap():
        try:
            func(*args, **kwargs)
        except KeyboardInterrupt:
            exit(-1)
    return wrap
