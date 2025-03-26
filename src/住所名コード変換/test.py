import os
import pathlib
import logging.config

print(os.path.dirname(__file__))

print(pathlib.Path(__file__))

print(pathlib.Path(__file__).absolute())

print(pathlib.Path(__file__).absolute() / 'config/logging.ini')

print(pathlib.Path(__file__).parent / 'config/logging.ini')

print(pathlib.Path(__file__).parents)

print(pathlib.Path(__file__).parent.absolute())
print(pathlib.Path(__file__).parent.parent.absolute())
print(pathlib.Path(__file__).parent.parent.parent.absolute())

print(pathlib.Path(__file__).resolve().parents[1])

print(type(pathlib.Path(__file__).resolve().parents[1]))