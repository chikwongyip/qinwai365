# coding: utf-8
from extract_handler import extract_handler

if __name__ == '__main__':
    # path = '/api/store/v1/queryStore'
    #     method_mode = 'MODIFY'
    extract_handler(path='/api/store/v1/queryStore', method_mode='MODIFY')
    # extract_handler(path='/api/store/v1/queryStore', method_mode='CREATE')
