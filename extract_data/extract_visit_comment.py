from extract_handler import extract_handler_visit

if __name__ == '__main__':
    # path = '/api/store/v1/queryStore'
    #     method_mode = 'MODIFY'
    extract_handler_visit(
        path='/api/cusVisit/v1/queryVisitApprovalByRecord', method_mode='VISIT')
    # extract_handler(path='/api/store/v1/queryStore', method_mode='CREATE')
