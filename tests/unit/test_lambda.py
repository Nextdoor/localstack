import copy
import json
import unittest
from localstack.services.awslambda import lambda_api, lambda_executors
from localstack.utils.aws import aws_models


class TestLambdaAPI(unittest.TestCase):
    CODE_SIZE = 50
    HANDLER = 'index.handler'
    RUNTIME = 'node.js4.3'
    TIMEOUT = 60  # Default value, hardcoded
    FUNCTION_NAME = 'test1'
    ALIAS_NAME = 'alias1'
    ALIAS2_NAME = 'alias2'
    RESOURCENOTFOUND_EXCEPTION = 'ResourceNotFoundException'
    RESOURCENOTFOUND_MESSAGE = 'Function not found: %s'
    ALIASEXISTS_EXCEPTION = 'ResourceConflictException'
    ALIASEXISTS_MESSAGE = 'Alias already exists: %s'
    ALIASNOTFOUND_EXCEPTION = 'ResourceNotFoundException'
    ALIASNOTFOUND_MESSAGE = 'Alias not found: %s'
    TEST_UUID = 'Test'

    def setUp(self):
        lambda_api.cleanup()
        self.maxDiff = None
        self.app = lambda_api.app
        self.app.testing = True
        self.client = self.app.test_client()

    def test_get_non_existent_function_returns_error(self):
        with self.app.test_request_context():
            result = json.loads(lambda_api.get_function('non_existent_function_name').get_data())
            self.assertEqual(self.RESOURCENOTFOUND_EXCEPTION, result['__type'])
            self.assertEqual(
                self.RESOURCENOTFOUND_MESSAGE % lambda_api.func_arn('non_existent_function_name'),
                result['message'])

    def test_get_event_source_mapping(self):
        with self.app.test_request_context():
            lambda_api.event_source_mappings.append({'UUID': self.TEST_UUID})
            result = lambda_api.get_event_source_mapping(self.TEST_UUID)
            self.assertEqual(json.loads(result.get_data()).get('UUID'), self.TEST_UUID)

    def test_delete_event_source_mapping(self):
        with self.app.test_request_context():
            lambda_api.event_source_mappings.append({'UUID': self.TEST_UUID})
            result = lambda_api.delete_event_source_mapping(self.TEST_UUID)
            self.assertEqual(json.loads(result.get_data()).get('UUID'), self.TEST_UUID)
            self.assertEqual(0, len(lambda_api.event_source_mappings))

    def test_publish_function_version(self):
        with self.app.test_request_context():
            self._create_function(self.FUNCTION_NAME)

            result = json.loads(lambda_api.publish_version(self.FUNCTION_NAME).get_data())
            result2 = json.loads(lambda_api.publish_version(self.FUNCTION_NAME).get_data())

            expected_fc1 = aws_models.FunctionConfiguration(
                code_size=self.CODE_SIZE,
                function_arn=str(lambda_api.func_arn(self.FUNCTION_NAME)) + ':1',
                function_name=str(self.FUNCTION_NAME),
                handler=str(self.HANDLER),
                runtime=str(self.RUNTIME),
                timeout=self.TIMEOUT,
                version='1')

            expected_fc2 = copy.deepcopy(expected_fc1)
            expected_fc2.FunctionArn = str(lambda_api.func_arn(self.FUNCTION_NAME)) + ':2'
            expected_fc2.Version = '2'
            self.assertDictEqual(expected_fc1.to_dict(), result)
            self.assertDictEqual(expected_fc2.to_dict(), result2)

    def test_publish_non_existant_function_version_returns_error(self):
        with self.app.test_request_context():
            result = json.loads(lambda_api.publish_version(self.FUNCTION_NAME).get_data())
            self.assertEqual(self.RESOURCENOTFOUND_EXCEPTION, result['__type'])
            self.assertEqual(self.RESOURCENOTFOUND_MESSAGE % lambda_api.func_arn(self.FUNCTION_NAME),
                             result['message'])

    def test_list_function_versions(self):
        with self.app.test_request_context():
            self._create_function(self.FUNCTION_NAME)
            lambda_api.publish_version(self.FUNCTION_NAME)
            lambda_api.publish_version(self.FUNCTION_NAME)

            result = json.loads(lambda_api.list_versions(self.FUNCTION_NAME).get_data())

            latest_config = aws_models.FunctionConfiguration(
                code_size=self.CODE_SIZE,
                function_arn=str(lambda_api.func_arn(self.FUNCTION_NAME)) + ':$LATEST',
                function_name=str(self.FUNCTION_NAME),
                handler=str(self.HANDLER),
                runtime=str(self.RUNTIME),
                timeout=self.TIMEOUT,
                version='$LATEST',
            )
            version1_config = copy.deepcopy(latest_config)
            version1_config.FunctionArn = str(lambda_api.func_arn(self.FUNCTION_NAME)) + ':1'
            version1_config.Version = '1'
            version2_config = copy.deepcopy(latest_config)
            version2_config.FunctionArn = str(lambda_api.func_arn(self.FUNCTION_NAME)) + ':2'
            version2_config.Version = '2'
            expected_result = {
                'Versions': sorted([
                    latest_config.to_dict(), version1_config.to_dict(), version2_config.to_dict()
                ], key=lambda k: str(k.get('Version')))}
            self.assertDictEqual(expected_result, result)

    def test_list_non_existant_function_versions_returns_error(self):
        with self.app.test_request_context():
            result = json.loads(lambda_api.list_versions(self.FUNCTION_NAME).get_data())
            self.assertEqual(self.RESOURCENOTFOUND_EXCEPTION, result['__type'])
            self.assertEqual(self.RESOURCENOTFOUND_MESSAGE % lambda_api.func_arn(self.FUNCTION_NAME),
                             result['message'])

    def test_create_alias(self):
        self._create_function(self.FUNCTION_NAME)
        self.client.post('{0}/functions/{1}/versions'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME))

        response = self.client.post('{0}/functions/{1}/aliases'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME),
                         data=json.dumps({'Name': self.ALIAS_NAME, 'FunctionVersion': '1',
                             'Description': ''}))
        result = json.loads(response.get_data())

        expected_result = {'AliasArn': lambda_api.func_arn(self.FUNCTION_NAME) + ':' + self.ALIAS_NAME,
                           'FunctionVersion': '1', 'Description': '', 'Name': self.ALIAS_NAME}
        self.assertDictEqual(expected_result, result)

    def test_create_alias_on_non_existant_function_returns_error(self):
        with self.app.test_request_context():
            result = json.loads(lambda_api.create_alias(self.FUNCTION_NAME).get_data())
            self.assertEqual(self.RESOURCENOTFOUND_EXCEPTION, result['__type'])
            self.assertEqual(self.RESOURCENOTFOUND_MESSAGE % lambda_api.func_arn(self.FUNCTION_NAME),
                             result['message'])

    def test_create_alias_returns_error_if_already_exists(self):
        self._create_function(self.FUNCTION_NAME)
        self.client.post('{0}/functions/{1}/versions'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME))
        data = json.dumps({'Name': self.ALIAS_NAME, 'FunctionVersion': '1', 'Description': ''})
        self.client.post('{0}/functions/{1}/aliases'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME), data=data)

        response = self.client.post('{0}/functions/{1}/aliases'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME),
                                    data=data)
        result = json.loads(response.get_data())

        alias_arn = lambda_api.func_arn(self.FUNCTION_NAME) + ':' + self.ALIAS_NAME
        self.assertEqual(self.ALIASEXISTS_EXCEPTION, result['__type'])
        self.assertEqual(self.ALIASEXISTS_MESSAGE % alias_arn,
                         result['message'])

    def test_update_alias(self):
        self._create_function(self.FUNCTION_NAME)
        self.client.post('{0}/functions/{1}/versions'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME))
        self.client.post('{0}/functions/{1}/aliases'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME),
                         data=json.dumps({
                             'Name': self.ALIAS_NAME, 'FunctionVersion': '1', 'Description': ''}))

        response = self.client.put('{0}/functions/{1}/aliases/{2}'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME,
                                                                          self.ALIAS_NAME),
                                   data=json.dumps({'FunctionVersion': '$LATEST', 'Description': 'Test-Description'}))
        result = json.loads(response.get_data())

        expected_result = {'AliasArn': lambda_api.func_arn(self.FUNCTION_NAME) + ':' + self.ALIAS_NAME,
                           'FunctionVersion': '$LATEST', 'Description': 'Test-Description',
                           'Name': self.ALIAS_NAME}
        self.assertDictEqual(expected_result, result)

    def test_update_alias_on_non_existant_function_returns_error(self):
        with self.app.test_request_context():
            result = json.loads(lambda_api.update_alias(self.FUNCTION_NAME, self.ALIAS_NAME).get_data())
            self.assertEqual(self.RESOURCENOTFOUND_EXCEPTION, result['__type'])
            self.assertEqual(self.RESOURCENOTFOUND_MESSAGE % lambda_api.func_arn(self.FUNCTION_NAME),
                             result['message'])

    def test_update_alias_on_non_existant_alias_returns_error(self):
        with self.app.test_request_context():
            self._create_function(self.FUNCTION_NAME)
            result = json.loads(lambda_api.update_alias(self.FUNCTION_NAME, self.ALIAS_NAME).get_data())
            alias_arn = lambda_api.func_arn(self.FUNCTION_NAME) + ':' + self.ALIAS_NAME
            self.assertEqual(self.ALIASNOTFOUND_EXCEPTION, result['__type'])
            self.assertEqual(self.ALIASNOTFOUND_MESSAGE % alias_arn, result['message'])

    def test_list_aliases(self):
        self._create_function(self.FUNCTION_NAME)
        self.client.post('{0}/functions/{1}/versions'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME))

        self.client.post('{0}/functions/{1}/aliases'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME),
                         data=json.dumps({'Name': self.ALIAS2_NAME, 'FunctionVersion': '$LATEST'}))
        self.client.post('{0}/functions/{1}/aliases'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME),
                         data=json.dumps({'Name': self.ALIAS_NAME, 'FunctionVersion': '1',
                                          'Description': self.ALIAS_NAME}))

        response = self.client.get('{0}/functions/{1}/aliases'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME))
        result = json.loads(response.get_data())
        expected_result = {'Aliases': [
            {
                'AliasArn': lambda_api.func_arn(self.FUNCTION_NAME) + ':' + self.ALIAS_NAME,
                'FunctionVersion': '1',
                'Name': self.ALIAS_NAME,
                'Description': self.ALIAS_NAME
            },
            {
                'AliasArn': lambda_api.func_arn(self.FUNCTION_NAME) + ':' + self.ALIAS2_NAME,
                'FunctionVersion': '$LATEST',
                'Name': self.ALIAS2_NAME,
                'Description': ''
            }
        ]}
        self.assertDictEqual(expected_result, result)

    def test_list_non_existant_function_aliases_returns_error(self):
        with self.app.test_request_context():
            result = json.loads(lambda_api.list_aliases(self.FUNCTION_NAME).get_data())
            self.assertEqual(self.RESOURCENOTFOUND_EXCEPTION, result['__type'])
            self.assertEqual(self.RESOURCENOTFOUND_MESSAGE % lambda_api.func_arn(self.FUNCTION_NAME),
                             result['message'])

    def test_get_container_name(self):
        executor = lambda_executors.EXECUTOR_CONTAINERS_REUSE
        name = executor.get_container_name('arn:aws:lambda:us-east-1:00000000:function:my_function_name')
        self.assertEqual(name, 'localstack_lambda_arn_aws_lambda_us-east-1_00000000_function_my_function_name')

    def test_put_concurrency(self):
        with self.app.test_request_context():
            self._create_function(self.FUNCTION_NAME)
            # note: PutFunctionConcurrency is mounted at: /2017-10-31
            # NOT lambda_api.PATH_ROOT
            # https://docs.aws.amazon.com/lambda/latest/dg/API_PutFunctionConcurrency.html
            concurrency_data = {'ReservedConcurrentExecutions': 10}
            response = self.client.put('/2017-10-31/functions/{0}/concurrency'.format(self.FUNCTION_NAME),
                                       data=json.dumps(concurrency_data))

            result = json.loads(response.get_data())
            self.assertDictEqual(concurrency_data, result)

    def test_concurrency_get_function(self):
        with self.app.test_request_context():
            self._create_function(self.FUNCTION_NAME)
            # note: PutFunctionConcurrency is mounted at: /2017-10-31
            # NOT lambda_api.PATH_ROOT
            # https://docs.aws.amazon.com/lambda/latest/dg/API_PutFunctionConcurrency.html
            concurrency_data = {'ReservedConcurrentExecutions': 10}
            self.client.put('/2017-10-31/functions/{0}/concurrency'.format(self.FUNCTION_NAME),
                            data=json.dumps(concurrency_data))

            response = self.client.get('{0}/functions/{1}'.format(lambda_api.PATH_ROOT, self.FUNCTION_NAME))

            result = json.loads(response.get_data())
            self.assertTrue('Concurrency' in result)
            self.assertDictEqual(concurrency_data, result['Concurrency'])

    def _create_function(self, function_name):
        arn = lambda_api.func_arn(function_name)
        config = aws_models.FunctionConfiguration(
            function_name=function_name,
            function_arn=arn,
            code_size=self.CODE_SIZE,
            handler=self.HANDLER,
            runtime=self.RUNTIME,
            timeout=self.TIMEOUT,
            version='$LATEST')
        mapping = aws_models.FunctionMapping(
            executor=None,
            function_configuration=config)
        lambda_api.arn_to_lambda[arn] = aws_models.LambdaFunction(arn, {'$LATEST': mapping})
