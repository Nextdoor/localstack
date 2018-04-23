import random
import json
from requests.models import Response

from localstack import config
from localstack import constants
from localstack.services.awslambda import lambda_api
from localstack.services.generic_proxy import ProxyListener
from localstack.utils.analytics import event_publisher
from localstack.utils.aws import aws_models
from localstack.utils.aws import aws_stack
from localstack.utils.common import to_str

# action headers
ACTION_PREFIX = 'Kinesis_20131202'
ACTION_PUT_RECORD = '%s.PutRecord' % ACTION_PREFIX
ACTION_PUT_RECORDS = '%s.PutRecords' % ACTION_PREFIX
ACTION_CREATE_STREAM = '%s.CreateStream' % ACTION_PREFIX
ACTION_DELETE_STREAM = '%s.DeleteStream' % ACTION_PREFIX


class ProxyListenerKinesis(ProxyListener):

    def forward_request(self, method, path, data, headers):
        data = json.loads(to_str(data))

        if random.random() < config.KINESIS_ERROR_PROBABILITY:
            return kinesis_error_response(data)
        return True

    def return_response(self, method, path, data, headers, response):
        action = headers.get('X-Amz-Target')
        data = json.loads(to_str(data))

        # Request modification should take place when the request was unsuccessful.
        if response.status_code > 299:
            return

        if action in (ACTION_CREATE_STREAM, ACTION_DELETE_STREAM):
            event_type = (event_publisher.EVENT_KINESIS_CREATE_STREAM if action == ACTION_CREATE_STREAM
                else event_publisher.EVENT_KINESIS_DELETE_STREAM)
            event_publisher.fire_event(event_type, payload={'n': event_publisher.get_hash(data.get('StreamName'))})
        elif action == ACTION_PUT_RECORD:
            stream_name = data['StreamName']
            response_body = json.loads(to_str(response.content))
            event_records = [create_kinesis_event(stream_name, data, response_body)]
            lambda_api.process_kinesis_records(event_records, stream_name)
        elif action == ACTION_PUT_RECORDS:
            response_body = json.loads(to_str(response.content))
            stream_name = data['StreamName']
            event_records = [
                create_kinesis_event(stream_name, record_data, response_data)
                for record_data, response_data in
                zip(data['Records'], response_body['Records'])]
            lambda_api.process_kinesis_records(event_records, stream_name)


def create_kinesis_event(stream_name, record_data, response_data):
    """Creates a LambdaKinesisEvent from a PutRecord request / response.

    :type stream_name: str
    :param stream_name: Name of thes tream.
    :type record_data: dict
    :param record_data: Record data sent to Kinesis
        (https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Record.html)
    :type response_data: dict
    :param response_data: Response data for an individual record.
        (https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecordsResultEntry.html)
    """
    stream_name = stream_name
    sequence_num = response_data.get('SequenceNumber')
    shard_id = response_data.get('ShardId')
    region = constants.DEFAULT_REGION
    return aws_models.LambdaKinesisEvent(
        aws_stack.kinesis_stream_arn(stream_name),
        event_id=':'.join((shard_id, sequence_num)),
        aws_region=region,
        data=record_data['Data'],
        partition_key=record_data['PartitionKey'],
        sequence_number=sequence_num)


# instantiate listener
UPDATE_KINESIS = ProxyListenerKinesis()


def kinesis_error_response(data):
    error_response = Response()
    error_response.status_code = 200
    content = {'FailedRecordCount': 1, 'Records': []}
    for record in data['Records']:
        content['Records'].append({
            'ErrorCode': 'ProvisionedThroughputExceededException',
            'ErrorMessage': 'Rate exceeded for shard X in stream Y under account Z.'
        })
    error_response._content = json.dumps(content)
    return error_response
