# main_test.py

from support.updater import Updater
from helpers.logging_helper import configure_logging, logger

configure_logging()

def main():
    Updater()

if __name__ == '__main__':
    main()