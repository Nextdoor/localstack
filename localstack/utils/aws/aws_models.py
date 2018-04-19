from __future__ import print_function

import time
import json
import six

if six.PY3:
    long = int


class Component(object):
    def __init__(self, id, env=None):
        self.id = id
        self.env = env
        self.created_at = None

    def name(self):
        return self.id

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.id)


class KinesisStream(Component):
    def __init__(self, id, params=None, num_shards=1, connection=None):
        super(KinesisStream, self).__init__(id)
        params = params or {}
        self.shards = []
        self.stream_name = params.get('name', self.name())
        self.num_shards = params.get('shards', num_shards)
        self.conn = connection
        self.stream_info = params

    def name(self):
        return self.id.split(':stream/')[-1]

    def connect(self, connection):
        self.conn = connection

    def describe(self):
        r = self.conn.describe_stream(StreamName=self.stream_name)
        return r.get('StreamDescription')

    def create(self, raise_on_error=False):
        try:
            self.conn.create_stream(StreamName=self.stream_name, ShardCount=self.num_shards)
        except Exception as e:
            # TODO catch stream already exists exception, otherwise rethrow
            if raise_on_error:
                raise e

    def get_status(self):
        description = self.describe()
        return description.get('StreamStatus')

    def put(self, data, key):
        if not isinstance(data, str):
            data = json.dumps(data)
        return self.conn.put_record(StreamName=self.stream_name, Data=data, PartitionKey=key)

    def read(self, amount=-1, shard='shardId-000000000001'):
        if not self.conn:
            raise Exception('Please create the Kinesis connection first.')
        s_iterator = self.conn.get_shard_iterator(self.stream_name, shard, 'TRIM_HORIZON')
        record = self.conn.get_records(s_iterator['ShardIterator'])
        while True:
            try:
                if record['NextShardIterator'] is None:
                    break
                else:
                    next_entry = self.conn.get_records(record['NextShardIterator'])
                    if len(next_entry['Records']):
                        print(next_entry['Records'][0]['Data'])
                    record = next_entry
            except Exception as e:
                print('Error reading from Kinesis stream "%s": %s' (self.stream_name, e))

    def wait_for(self):
        GET_STATUS_SLEEP_SECS = 5
        GET_STATUS_RETRIES = 50
        for i in range(0, GET_STATUS_RETRIES):
            try:
                status = self.get_status()
                if status == 'ACTIVE':
                    return
            except Exception:
                # swallowing this exception should be ok, as we are in a retry loop
                pass
            time.sleep(GET_STATUS_SLEEP_SECS)
        raise Exception('Failed to get active status for stream "%s", giving up' % self.stream_name)

    def destroy(self):
        self.conn.delete_stream(StreamName=self.stream_name)
        time.sleep(2)


class KinesisShard(Component):
    MAX_KEY = '340282366920938463463374607431768211455'

    def __init__(self, id):
        super(KinesisShard, self).__init__(id)
        self.stream = None
        self.start_key = '0'
        self.end_key = KinesisShard.MAX_KEY  # 128 times '1' binary as decimal
        self.child_shards = []

    def print_tree(self, indent=''):
        print('%s%s' % (indent, self))
        for c in self.child_shards:
            c.print_tree(indent=indent + '   ')

    def length(self):
        return long(self.end_key) - long(self.start_key)

    def percent(self):
        return 100.0 * self.length() / float(KinesisShard.MAX_KEY)

    def __str__(self):
        return ('Shard(%s, length=%s, percent=%s, start=%s, end=%s)' %
                (self.id, self.length(), self.percent(), self.start_key,
                    self.end_key))

    @staticmethod
    def sort(shards):
        def compare(x, y):
            s1 = long(x.start_key)
            s2 = long(y.start_key)
            if s1 < s2:
                return -1
            elif s1 > s2:
                return 1
            else:
                return 0
        return sorted(shards, cmp=compare)

    @staticmethod
    def max(shards):
        max_shard = None
        max_length = long(0)
        for s in shards:
            if s.length() > max_length:
                max_shard = s
                max_length = s.length()
        return max_shard


class FirehoseStream(KinesisStream):
    def __init__(self, id):
        super(FirehoseStream, self).__init__(id)
        self.destinations = []

    def name(self):
        return self.id.split(':deliverystream/')[-1]


class LambdaKinesisEvent(object):
    """An event provided to a Lambda function from a Kinesis source.

    https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-kinesis-streams
    """
    def __init__(self, event_source_arn, event_id, aws_region,
                 data, partition_key, sequence_number):
        """Initializes the event.

        :type event_source_arn: str
        :param event_source_arn: ARN of the stream.
        :type event_id: str
        :param event_id: ID of the event (shardId-<shard_id>:<sequence_number>).
        :type aws_region: str
        :param aws_region: Region the Kinesis event is sent from.
        :type data: str
        :param data: Kinesis data.
        :type partition_key: str
        :param partition_key: Kinesis partition key.
        :type sequence_number: str
        :param sequence_number: Kinesis sequence number.
        """
        self.event_source_arn = event_source_arn
        self.event_id = event_id
        self.aws_region = aws_region
        self.data = data
        self.partition_key = partition_key
        self.sequence_number = sequence_number

    def to_dict(self):
        return {
            'eventVersion': '1.0',
            'eventName': 'aws:kinesis:record',
            'eventSource': 'aws:kinesis',
            'eventSourceARN': self.event_source_arn,
            'eventID': self.event_id,
            'awsRegion': self.aws_region,
            'kinesis': {
                'kinesisSchemaVersion': '1.0',
                'data': self.data,
                'partitionKey': self.partition_key,
                'sequenceNumber': self.sequence_number
            }
        }


class LambdaFunction(Component):
    def __init__(self, arn, versions):
        """Initializes a LambdaFunction.

        Args:
            arn (str): ARN of the function.
            versions (dict string FunctionMapping):
                A mapping of version names to FunctionMappings.
        """
        super(LambdaFunction, self).__init__(arn)
        self.event_sources = []
        self.targets = []
        self.cwd = None
        self.versions = versions
        self.aliases = {}
        self.concurrency = None

    def get_version(self, version):
        return self.versions.get(version)

    def name(self):
        return self.versions['$LATEST'].function_configuration.FunctionName

    def arn(self):
        return self.id

    def qualifier_exists(self, qualifier):
        return qualifier in self.aliases or qualifier in self.versions

    def latest(self):
        """Retrieves the '$LATEST' FunctionMapping

        Returns:
            FunctionMapping
        """
        return self.versions['$LATEST']

    def set_latest(self, function_mapping):
        """Sets the '$LATEST' FunctionMapping.

        Args:
            function_mapping (FunctionMapping): Function mapping to set for 'latest'.
        """
        self.versions['$LATEST'] = function_mapping

    def function(self, qualifier=None):
        if not qualifier:
            qualifier = '$LATEST'
        version = qualifier if qualifier in self.versions else \
            self.aliases.get(qualifier).get('FunctionVersion')
        return self.versions.get(version).get('Function')

    def __str__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.name())


class FunctionMapping(object):
    """A function handler and configuration."""
    def __init__(self, executor, working_directory=None, function_configuration=None):
        """Initializes a FunctionHandler.

        Args:
            executor (func): Function that handles the lambda.

        Keyword Args:
            working_directory (str): Directory to run the function in.
            function_configuration (FunctionConfiguration): Configuration of the function.
        """
        self.executor = executor
        self.working_directory = working_directory
        self.function_configuration = function_configuration


class DictSerializable(object):
    """A superclass that automatically renders all object properties to a dict."""
    def to_dict(self):
        result = {}
        for key, value in self.__dict__.items():
            if value is None:
                continue
            try:
                result[key] = value.to_dict()
            except AttributeError:
                result[key] = value
        return result


class FunctionConfiguration(DictSerializable):
    """https://docs.aws.amazon.com/lambda/latest/dg/API_FunctionConfiguration.html"""
    def __init__(self, code_sha_256=None, code_size=None, dead_letter_config=None,
                 description=None, environment=None, function_arn=None, function_name=None,
                 handler=None, kms_key_arn=None, last_modified=None, master_arn=None,
                 memory_size=None, revision_id=None, role=None, runtime=None, timeout=300,
                 tracing_config=None, version=None, vpc_config=None):
        self.CodeSha256 = code_sha_256
        self.CodeSize = code_size
        self.DeadLetterConfig = dead_letter_config
        self.Description = description
        self.Environment = environment
        self.FunctionArn = function_arn
        self.FunctionName = function_name
        self.Handler = handler
        self.KMSKeyArn = kms_key_arn
        self.LastModified = last_modified
        self.MasterArn = master_arn
        self.MemorySize = memory_size
        self.RevisionId = revision_id
        self.Role = role
        self.Runtime = runtime
        self.Timeout = timeout
        self.TracingConfig = tracing_config
        self.Version = version
        self.VpcConfig = vpc_config

    def to_dict(self, latest_arn=False):
        """Converts a FunctionConfiguration to a dict

        Keyword Args:
            latest_arn (bool): Whether or not to add '$LATEST' to the FunctionArn.
                Note: For non-latest versions, the version suffix, is required.
        """
        result = super(FunctionConfiguration, self).to_dict()
        if latest_arn and self.Version == '$LATEST':
            result['FunctionArn'] += ':$LATEST'
        return result

    @classmethod
    def from_json(cls, json_data):
        environment = None
        if 'Environment' in json_data:
            environment = Environment.from_json(json_data['Environment'])

        dead_letter_config = None
        if 'DeadLetterConfig' in json_data:
            dead_letter_config = DeadLetterConfig.from_json(json_data['DeadLetterConfig'])

        tracing_config = None
        if 'TracingConfig' in json_data:
            tracing_config = TracingConfig.from_json(json_data['TracingConfig'])

        vpc_config = None
        if 'VpcConfig' in json_data:
            vpc_config = VpcConfig.from_json(json_data['VpcConfig'])

        return cls(
            code_sha_256=json_data.get('CodeSha256'),
            code_size=json_data.get('CodeSize'),
            dead_letter_config=dead_letter_config,
            description=json_data.get('Description'),
            environment=environment,
            function_arn=json_data.get('FunctionArn'),
            function_name=json_data.get('FunctionName'),
            handler=json_data.get('Handler'),
            kms_key_arn=json_data.get('KMSKeyArn'),
            last_modified=json_data.get('LastModified'),
            master_arn=json_data.get('MasterArn'),
            memory_size=json_data.get('MemorySize'),
            revision_id=json_data.get('RevisionId'),
            role=json_data.get('Role'),
            runtime=json_data.get('Runtime'),
            timeout=json_data.get('Timeout'),
            tracing_config=tracing_config,
            version=json_data.get('Version'),
            vpc_config=vpc_config)


class Environment(DictSerializable):
    """https://docs.aws.amazon.com/lambda/latest/dg/API_EnvironmentResponse.html"""
    def __init__(self, error=None, variables=None):
        self.Error = error
        self.Variables = variables

    @classmethod
    def from_json(cls, json_data):
        return cls(error=json_data.get('Error'), variables=json_data.get('Variables'))


class DeadLetterConfig(DictSerializable):
    """https://docs.aws.amazon.com/lambda/latest/dg/API_DeadLetterConfig.html"""
    def __init__(self, target_arn=None):
        self.TargetArn = target_arn

    @classmethod
    def from_json(cls, json_data):
        return cls(target_arn=json_data['TargetArn'])


class TracingConfig(DictSerializable):
    """https://docs.aws.amazon.com/lambda/latest/dg/API_TracingConfig.html"""
    def __init__(self, mode=None):
        self.Mode = mode

    @classmethod
    def from_json(cls, json_data):
        return cls(mode=json_data['Mode'])


class VpcConfig(DictSerializable):
    """https://docs.aws.amazon.com/lambda/latest/dg/API_VpcConfigResponse.html"""
    def __init__(self, security_group_ids=None, subnet_ids=None, vpc_id=None):
        self.SecurityGroupIds = security_group_ids
        self.SubnetIds = subnet_ids
        self.VpcId = vpc_id

    @classmethod
    def from_json(cls, json_data):
        return cls(
            security_group_ids=json_data['SecurityGroupIds'],
            subnet_ids=json_data['SubnetIds'],
            vpc_id=json_data['VpcId'])


class DynamoDB(Component):
    def __init__(self, id, env=None):
        super(DynamoDB, self).__init__(id, env=env)
        self.count = -1
        self.bytes = -1

    def name(self):
        return self.id.split(':table/')[-1]


class DynamoDBStream(Component):
    def __init__(self, id):
        super(DynamoDBStream, self).__init__(id)
        self.table = None


class DynamoDBItem(Component):
    def __init__(self, id, table=None, keys=None):
        super(DynamoDBItem, self).__init__(id)
        self.table = table
        self.keys = keys

    def __eq__(self, other):
        if not isinstance(other, DynamoDBItem):
            return False
        return (other.table == self.table and
            other.id == self.id and
            other.keys == self.keys)

    def __hash__(self):
        return hash(self.table) + hash(self.id) + hash(self.keys)


class ElasticSearch(Component):
    def __init__(self, id):
        super(ElasticSearch, self).__init__(id)
        self.indexes = []
        self.endpoint = None

    def name(self):
        return self.id.split(':domain/')[-1]


class SqsQueue(Component):
    def __init__(self, id):
        super(SqsQueue, self).__init__(id)

    def name(self):
        return self.id.split(':')[-1]


class S3Bucket(Component):
    def __init__(self, id):
        super(S3Bucket, self).__init__(id)
        self.notifications = []

    def name(self):
        return self.id.split('arn:aws:s3:::')[-1]


class S3Notification(Component):
    def __init__(self, id):
        super(S3Notification, self).__init__(id)
        self.target = None
        self.trigger = None


class EventSource(Component):
    def __init__(self, id):
        super(EventSource, self).__init__(id)

    @staticmethod
    def get(obj, pool=None, type=None):
        pool = pool or {}
        if not obj:
            return None
        if isinstance(obj, Component):
            obj = obj.id
        if obj in pool:
            return pool[obj]
        inst = None
        if obj.startswith('arn:aws:kinesis:'):
            inst = KinesisStream(obj)
        if obj.startswith('arn:aws:lambda:'):
            inst = LambdaFunction(obj)
        elif obj.startswith('arn:aws:dynamodb:'):
            if '/stream/' in obj:
                table_id = obj.split('/stream/')[0]
                table = DynamoDB(table_id)
                inst = DynamoDBStream(obj)
                inst.table = table
            else:
                inst = DynamoDB(obj)
        elif type:
            for o in EventSource.filter_type(pool, type):
                if o.name() == obj:
                    return o
                if type == ElasticSearch:
                    if o.endpoint == obj:
                        return o
        else:
            print("Unexpected object name: '%s'" % obj)
        return inst

    @staticmethod
    def filter_type(pool, type):
        return [obj for obj in six.itervalues(pool) if isinstance(obj, type)]
