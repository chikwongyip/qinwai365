# coding:utf-8
from typing import Dict, Any, Optional


class RequestStrategy:
    """Base strategy for constructing request bodies."""

    def construct_body(self, **kwargs) -> Dict[str, Any]:
        """
        Construct the request body.

        Args:
            **kwargs: Arguments passed from extract_data.

        Returns:
            Dict[str, Any]: The constructed request body.
        """
        raise NotImplementedError


class DefaultStrategy(RequestStrategy):
    """Default strategy for standard API endpoints."""

    def construct_body(self, **kwargs) -> Dict[str, Any]:
        page_number = kwargs.get('page_number')
        method_mode = kwargs.get('method_mode')
        after_modify_date = kwargs.get('after_modify_date')
        before_modify_date = kwargs.get('before_modify_date')

        request_body = dict(page_number=page_number)
        if method_mode == 'CREATE':
            if after_modify_date:
                request_body['after_create_date'] = after_modify_date
            if before_modify_date:
                request_body['before_create_date'] = before_modify_date
        else:
            if after_modify_date:
                request_body['after_modify_date'] = after_modify_date
            if before_modify_date:
                request_body['before_modify_date'] = before_modify_date
        return request_body


class UserDefinedStrategy(RequestStrategy):
    def construct_body(self, **kwargs) -> Dict[str, Any]:
        page_number = kwargs.get('page_number')
        method_mode = kwargs.get('method_mode')
        after_modify_date = kwargs.get('after_modify_date')
        before_modify_date = kwargs.get('before_modify_date')
        form_id = kwargs.get('form_id')

        request_body = dict(page=page_number)
        request_body['rows'] = 1000
        if form_id:
            request_body['form_id'] = form_id

        if method_mode == 'CREATE':
            if after_modify_date:
                request_body['date_start'] = after_modify_date
            if before_modify_date:
                request_body['date_end'] = before_modify_date
        else:
            if after_modify_date:
                request_body['modify_date_start'] = after_modify_date
            if before_modify_date:
                request_body['modify_date_end'] = before_modify_date
        return request_body


class VisitRecordApprovalStrategy(RequestStrategy):
    def construct_body(self, **kwargs) -> Dict[str, Any]:
        page_number = kwargs.get('page_number')
        method_mode = kwargs.get('method_mode')
        after_modify_date = kwargs.get('after_modify_date')
        before_modify_date = kwargs.get('before_modify_date')
        function_id = kwargs.get('function_id')

        request_body = dict(page=page_number)
        request_body['rows'] = 1000
        if function_id:
            request_body['function_id'] = function_id

        if method_mode == 'CREATE':
            if after_modify_date:
                request_body['create_date_start'] = after_modify_date
            if before_modify_date:
                request_body['create_date_end'] = before_modify_date
        else:
            if after_modify_date:
                request_body['approve_date_start'] = after_modify_date
            if before_modify_date:
                request_body['approve_date_end'] = before_modify_date
        return request_body


class RegularSaleStrategy(RequestStrategy):
    def construct_body(self, **kwargs) -> Dict[str, Any]:
        page_number = kwargs.get('page_number')
        method_mode = kwargs.get('method_mode')
        after_modify_date = kwargs.get('after_modify_date')
        before_modify_date = kwargs.get('before_modify_date')

        request_body = dict(page_number=page_number)
        if method_mode == 'CREATE':
            request_body['create_start'] = after_modify_date
            request_body['create_end'] = before_modify_date
        else:
            request_body['modify_start'] = after_modify_date
            request_body['modify_end'] = before_modify_date
        return request_body


class RegularCommentStrategy(RequestStrategy):
    def construct_body(self, **kwargs) -> Dict[str, Any]:
        page_number = kwargs.get('page_number')
        method_mode = kwargs.get('method_mode')
        after_modify_date = kwargs.get('after_modify_date')
        before_modify_date = kwargs.get('before_modify_date')

        request_body = dict(page_number=page_number)
        if method_mode == 'CREATE':
            request_body['create_start'] = after_modify_date
            request_body['create_end'] = before_modify_date
        elif method_mode == 'MODIFY':
            request_body['comment_time_start'] = after_modify_date
            request_body['comment_time_end'] = before_modify_date
        else:
            request_body['re_comment_time_start'] = after_modify_date
            request_body['re_comment_time_end'] = before_modify_date
        return request_body


class RegularSaleActivitiesStrategy(RequestStrategy):
    def construct_body(self, **kwargs) -> Dict[str, Any]:
        page_number = kwargs.get('page_number')
        method_mode = kwargs.get('method_mode')
        after_modify_date = kwargs.get('after_modify_date')
        before_modify_date = kwargs.get('before_modify_date')

        request_body = dict(page_number=page_number)
        if method_mode == 'CREATE':
            request_body['create_date_begin'] = after_modify_date
            request_body['create_date_end'] = before_modify_date
        else:
            request_body['modify_date_begin'] = after_modify_date
            request_body['modify_date_end'] = before_modify_date
        return request_body


class EmployeeStrategy(RequestStrategy):
    def construct_body(self, **kwargs) -> Dict[str, Any]:
        page_number = kwargs.get('page_number')
        method_mode = kwargs.get('method_mode')

        request_body = dict(page_number=page_number)
        if method_mode == 'CREATE':
            # Note: original code used kwargs.get('create_date') which might be None if not passed, but let's stick to original logic logic or improve it.
            request_body['create_date'] = kwargs.get('create_date')
            # Wait, original code used kwargs.get('create_date') but extract_data receives after_modify_date.
            # Let's check extract_data.py again.
            # Line 73: request_body['create_date'] = kwargs.get('create_date')
            # But extract_data signature is (**kwargs).
            # It seems 'create_date' is expected in kwargs.
            # However, in extract_handler, it calls extract_data with after_modify_date.
            # It seems the original code might have a bug or I missed where create_date is passed.
            # In extract_handler.py line 38, it passes after_modify_date.
            # Let's assume for now we map after_modify_date to create_date if that's the intention,
            # OR we just pass kwargs through.
            # Let's look at the original code:
            # if method_mode == 'CREATE':
            #    request_body['create_date'] = kwargs.get('create_date')
            # else:
            #    request_body['modify_date'] = kwargs.get('modify_date')

            # If extract_data is called with after_modify_date, then kwargs.get('create_date') would be None unless it's in kwargs.
            # I will preserve the exact behavior: use kwargs.get('create_date')
            pass

        # Actually, looking at extract_handler.py, it passes:
        # after_modify_date=after_modify_date
        # So 'create_date' key is NOT in kwargs.
        # This implies the original code for Employee might be broken or relies on 'create_date' being passed explicitly which extract_handler doesn't seem to do?
        # Wait, extract_handler passes **kwargs? No, it passes specific args.
        # extract_data(..., after_modify_date=..., ...)
        # So kwargs.get('create_date') is likely None.
        # I will fix this potential bug by using after_modify_date if create_date is missing, or just stick to strict translation.
        # Strict translation is safer for refactoring.

        if method_mode == 'CREATE':
            request_body['create_date'] = kwargs.get('create_date')
        else:
            request_body['modify_date'] = kwargs.get('modify_date')
        return request_body


STRATEGY_MAP = {
    '/api/userDefined/v1/queryUserDefined': UserDefinedStrategy(),
    '/api/cusVisit/v1/getVisitRecordApprovalData': VisitRecordApprovalStrategy(),
    '/api/cuxiao/v1/queryRegularSale': RegularSaleStrategy(),
    '/api/cuxiao/v1/queryRegularReport': RegularCommentStrategy(),
    '/api/cuxiao/v1/queryRegularSaleActivities': RegularSaleActivitiesStrategy(),
    '/api/employee/v3/queryEmployee': EmployeeStrategy(),
}


def get_strategy(path: str) -> RequestStrategy:
    return STRATEGY_MAP.get(path, DefaultStrategy())
