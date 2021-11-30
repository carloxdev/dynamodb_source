# Python's Libraries
import uuid

# Third-party Libraries
from unittest import TestCase
# from unittest import mock
from dotenv import load_dotenv

# Own's Libraries
from stxlibs.logger import Logger
from dynamodb_source import DynamoDBSource


load_dotenv()


class DynamodbSource(TestCase):

    def test_GetOne_Success(self):
        logger = Logger.create()

        filters = {
            "uuid": "e7362c92-5175-11ec-8aa2-b630403e3bed"
        }

        src = DynamoDBSource(_logger=logger)
        data = src.select_One("traffic", filters)

        self.assertEqual(type(data), dict)

    def test_GetAll_Success(self):
        logger = Logger.create()

        index_name = "get-type-and-user"

        conditions_values = {
            ':record_type': {
                'S': "invoice"
            }
        }

        conditions = 'record_type = :record_type'

        src = DynamoDBSource(_logger=logger)
        data = src.select_Many(
            "traffic",
            conditions,
            conditions_values,
            _index_name=index_name
        )

        self.assertEqual(type(data), dict)

    def test_Add_Success(self):
        logger = Logger.create()

        key = uuid.uuid1()
        data = {
            "uuid": str(key),
            "rec_cre_usr": "jorge.gomez",
            "record_type": "invoice"
        }

        src = DynamoDBSource(_logger=logger)
        response = src.add("traffic", data)

        self.assertEqual(response, True)

    def test_Update_Success(self):
        logger = Logger.create()
        filter = {
            "uuid": "e7362c92-5175-11ec-8aa2-b630403e3bed"
        }
        expression = "set rec_cre_usr=:rec_cre_usr"
        expression_values = {
            ':rec_cre_usr': "jorge.gomez (updated 2)"
        }

        src = DynamoDBSource(_logger=logger)
        response = src.update("traffic", filter, expression, expression_values)

        self.assertEqual(response, True)
