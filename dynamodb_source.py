# Python's Libraries
import logging

# Third-party Libraries
import boto3
from botocore.exceptions import NoCredentialsError

# Own's Libraries
from stxlibs.errors import SourceError
from stxlibs.errors import NoRecordFoundError
from stxlibs.errors import NoRecordsFoundError


class DynamoDBSource(object):

    def __init__(self, _logger=None, _url=None):
        self.logger = _logger or logging.getLogger(__name__)
        self.client = None
        self.url = _url

    def __connect_WithResource(self):
        try:
            if self.url:
                self.client = boto3.resource(
                    'dynamodb',
                    endpoint_url=self.url
                )
            else:
                self.client = boto3.resource('dynamodb')

        except NoCredentialsError as e:
            raise SourceError(
                _message="dynamodb wrong credentials",
                _error=str(e),
                _logger=self.logger
            )

        except Exception as e:
            raise SourceError(
                _message=str(e),
                _logger=self.logger,
                _error=str(e)
            )

    def __connect(self):
        try:
            if self.url:
                self.client = boto3.client(
                    'dynamodb',
                    endpoint_url="http://localhost:8000"
                )

            else:
                self.client = boto3.client(
                    'dynamodb'
                )

        except NoCredentialsError as e:
            raise SourceError(
                _message="dynamodb wrong credentials",
                _error=str(e),
                _logger=self.logger
            )

        except Exception as e:
            raise SourceError(
                _message=str(e),
                _logger=self.logger,
                _error=str(e)
            )

    def select_One(self, _table_name, _filters):
        self.logger.info(f"Searching record with filters: {_filters}")
        self.__connect_WithResource()

        try:
            table = self.client.Table(_table_name)
            response = table.get_item(
                Key=_filters
            )

        except Exception as e:
            raise SourceError(
                _message=str(e),
                _error=str(e),
                _logger=self.logger
            )

        data = {}
        if 'Item' in response:
            data = response['Item']

        else:
            raise NoRecordFoundError(
                _message="Record not found",
                _logger=self.logger
            )

        return data

    def add(self, _table_name, _data):
        self.logger.info(f"Adding data to {_table_name}: {_data}")
        self.__connect_WithResource()

        try:
            table = self.client.Table(_table_name)
            response = table.put_item(
                Item=_data
            )

        except Exception as e:
            raise SourceError(
                _message=str(e),
                _error=str(e),
                _logger=self.logger
            )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True

        else:
            raise SourceError(
                _message=response,
                _logger=self.logger
            )

    def update(self, _table_name, _keys, _expresion, _expresion_values):
        self.logger.info(
            f"Updating record in {_table_name} with keys {_keys}: {_expresion_values}"
        )
        self.__connect_WithResource()

        try:
            table = self.client.Table(_table_name)
            response = table.update_item(
                Key=_keys,
                UpdateExpression=_expresion,
                ExpressionAttributeValues=_expresion_values,
                ReturnValues="UPDATED_NEW"
            )

        except Exception as e:
            raise SourceError(
                _message=str(e),
                _error=str(e),
                _logger=self.logger
            )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True

        else:
            raise SourceError(
                _message=response,
                _logger=self.logger
            )

    def select_Many(
        self,
        _table_name,
        _keyconditions,
        _keyconditions_values,
        _attributes_names=None,
        _filters=None,
        _index_name=None,
        _start_key=None,
        _page_size=None
    ):
        self.logger.info(f"<--- Query in table: {_table_name}")
        if _keyconditions is None or _keyconditions_values is None:
            raise SourceError(
                _message="KeyConditionExpression is missing",
                _error=None,
                _logger=self.logger
            )

        arguments = {}
        arguments['TableName'] = _table_name
        arguments['KeyConditionExpression'] = _keyconditions
        arguments['ExpressionAttributeValues'] = _keyconditions_values
        arguments['PaginationConfig'] = {
            'PageSize': _page_size,
            'StartingToken': None
        }

        if _index_name:
            arguments['IndexName'] = _index_name

        if _filters:
            arguments['FilterExpression'] = _filters

        if _attributes_names:
            arguments['ExpressionAttributeNames'] = _attributes_names

        if _start_key:
            arguments['ExclusiveStartKey'] = _start_key

        try:
            self.__connect()
            paginator = self.client.get_paginator('query')

            self.logger.info(f"Using arguments: {arguments}")
            page_iterator = paginator.paginate(**arguments)

            data = {}
            data['Items'] = []
            first = True
            count = 0

            for page in page_iterator:
                count += 1
                self.logger.info(f"{len(page['Items'])} Records found in page {count}")

                if first:
                    data['Items'] = page['Items']
                    first = False

                else:
                    data['Items'] += page['Items']

                if 'LastEvaluatedKey' in page:
                    data['LastEvaluatedKey'] = page['LastEvaluatedKey']

            self.logger.info(f"Records found in {count} request: {len(data)}")

        except Exception as e:
            raise SourceError(
                _message=str(e),
                _error=str(e),
                _logger=self.logger
            )

        if len(data['Items']) == 0:
            raise NoRecordsFoundError(
                _message="No records found",
                _logger=self.logger
            )

        return data

    def select_ManyWithScan_DbPag(
        self,
        _model,
        _keyconditions=None,
        _keyconditions_values=None,
        _attributes_names=None,
        _index_name=None,
        _start_key=None,
        _page_size=None
    ):
        print(f"<--- Scan in table: {_model.__tablename__}")

        arguments = {}
        arguments['TableName'] = _model.__tablename__
        arguments['PaginationConfig'] = {
            'PageSize': _page_size
        }

        if _keyconditions:
            arguments['FilterExpression'] = _keyconditions
            arguments['ExpressionAttributeValues'] = _keyconditions_values

        if _index_name:
            arguments['IndexName'] = _index_name

        if _attributes_names:
            arguments['ExpressionAttributeNames'] = _attributes_names

        if _start_key:
            arguments['ExclusiveStartKey'] = _start_key

        try:
            self.__connect()
            paginator = self.client.get_paginator('scan')

            print("Argumentos utilizados .......")
            print(arguments)
            page_iterator = paginator.paginate(**arguments)

            qty = 0
            missing = 0
            limit = _page_size
            data = []
            first = True
            last_evalued = None
            count = 0

            for page in page_iterator:
                count += 1
                print(f"{len(page['Items'])} Records found in page {count}")

                if first:
                    data = page['Items']
                    first = False

                for i in range(missing):
                    dta = page['Items']
                    if (i + 1) <= page['Count']:
                        data.append(dta[i])

                qty = len(data)

                if 'LastEvaluatedKey' not in page:
                    break

                if qty == limit:
                    last_evalued = data[-1]
                    missing = 0
                    break

                missing = limit - qty

            print(f"Records found in {count} request: {len(data)}")

        except Exception as e:
            raise SourceError(
                _message=str(e),
                _error=str(e),
                _logger=self.logger
            )

        if len(data) == 0:
            raise NoRecordsFoundError(
                _message="No records found",
                _logger=self.logger
            )

        return_data = {}
        return_data['Items'] = data
        return_data['LastEvaluatedKey'] = last_evalued

        return return_data

    def select_ManyWithScan_LambdaPag(
        self,
        _model,
        _keyconditions=None,
        _keyconditions_values=None,
        _attributes_names=None,
        _index_name=None,
        _start_key=None
    ):
        print(f"<--- Scan in table: {_model.__tablename__}")

        arguments = {}
        arguments['TableName'] = _model.__tablename__

        if _keyconditions:
            arguments['FilterExpression'] = _keyconditions
            arguments['ExpressionAttributeValues'] = _keyconditions_values

        if _index_name:
            arguments['IndexName'] = _index_name

        if _attributes_names:
            arguments['ExpressionAttributeNames'] = _attributes_names

        if _start_key:
            arguments['ExclusiveStartKey'] = _start_key

        try:
            self.__connect()
            paginator = self.client.get_paginator('scan')

            print("Argumentos utilizados .......")
            print(arguments)
            page_iterator = paginator.paginate(**arguments)

            data = []
            first = True
            last_evalued = None
            count = 0

            for page in page_iterator:
                count += 1
                print(f"{len(page['Items'])} Records found in page {count}")

                if first:
                    data = page['Items']
                    first = False

                else:
                    data += page['Items']

            print(f"Records found in {count} request: {len(data)}")

        except Exception as e:
            raise SourceError(
                _message=str(e),
                _error=str(e),
                _logger=self.logger
            )

        if len(data) == 0:
            raise NoRecordsFoundError(
                _message="No records found",
                _logger=self.logger
            )

        else:
            data_pag = data[:10]
            last_evalued = data_pag[-1]

        return_data = {}
        return_data['Items'] = data_pag
        return_data['LastEvaluatedKey'] = last_evalued

        return return_data
