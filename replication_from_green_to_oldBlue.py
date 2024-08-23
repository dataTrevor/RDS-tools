import sys
import logging
import pymysql
import json
import os
import boto3
import time

boto3.set_stream_logger(name='botocore.credentials', level=logging.INFO)
# rds user settings of old blue
source_rds_port = 3306
user_name = os.environ['USER_NAME']
password = os.environ['PASSWORD']
#custerID to switch over
clusterId = os.environ['CLUSTER_ID']
clusterRegion = os.environ['REGION']
repl_user = os.environ['REPL_USER']
repl_pass = os.environ['REPL_PASS']
binlog_msg = os.environ['BINLOG_MSG']
#now, cdc_method support 2 values: MY_REPLICATION, DMS_CDC
cdc_method = os.environ['CDC_METHOD']
repInstanceArn = os.environ['REP_INSTANCE_ARN']

start_slave_sql = 'CALL mysql.rds_start_replication'
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
def getConnection(rds_host, user_name, password, db_name=None):
    try:
        return pymysql.connect(host=rds_host, user=user_name, passwd=password, db=db_name, connect_timeout=30, autocommit=True)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit(1)

def getClient(srvName, region_name):
    client = boto3.client(srvName, region_name)
    return client

# get Aurora Instance list from AWS SDK by clusterID, not for RDS instance!
def getCluster(client, clusterId):
    instanceList = []
    dbclusters = []
    dbInstances = []
    if clusterId != None:
        response = client.describe_db_clusters(
            DBClusterIdentifier = clusterId,
            Marker = 'string'
            )
        dbclusters = response['DBClusters']
        dbcluster = dbclusters[0]
    return dbcluster
    
def deleteBlueGreenDeployment(client, blueGreenDeploymentIdentifier: str):
    response = client.delete_blue_green_deployment(
        BlueGreenDeploymentIdentifier=blueGreenDeploymentIdentifier,
        DeleteTarget=False
    )
    return response['BlueGreenDeployment']

def descBlueGreenDeployment(client, blueGreenDeploymentIdentifier: str):
    response = client.describe_blue_green_deployments(
        BlueGreenDeploymentIdentifier=blueGreenDeploymentIdentifier,
        MaxRecords=20
    )
    return response['BlueGreenDeployments'][0]
    
def ListEvents(client, sourceType: str, sourceIdentifier: str, startTime, endTime, eventCategory):
    response = client.describe_events(
    SourceIdentifier=sourceIdentifier,
    SourceType=sourceType,
    StartTime=startTime,
    EndTime=endTime,
    MaxRecords=100
    )
    return response["Events"]
    
def ListEventsWithoutId(client, sourceType: str, startTime, endTime, eventCategory):
    response = client.describe_events(
    SourceType=sourceType,
    StartTime=startTime,
    EndTime=endTime,
    MaxRecords=100
    )
    return response["Events"]

# get binlog file and pos from :"Binary log coordinates in green environment after switchover: file mysql-bin-changelog.000007 and position 536"
def getBinlogFilePos(binlogMsg: str):
    fileInx = binlogMsg.index('file')
    andInx = binlogMsg.index('and')
    positionInx = binlogMsg.index('position')
    fileStartInx = fileInx + len('file') + 1
    posStartInx = positionInx + len('position') + 1
    binlogFile = binlogMsg[fileStartInx: andInx]
    binlogPos = binlogMsg[posStartInx:]
    return binlogFile.strip(), int(binlogPos)

# create the MySQLSetting in DMS endpoint
def MySQLSettingsJson(servername, port, username, password):
    settingJson = {'ServerTimezone': 'UTC'}
    settingJson['ServerName'] = servername
    settingJson['Port'] = port
    settingJson['Username'] = username
    settingJson['Password'] = password

    return settingJson

# init the tableMappingJson for CDC task
def initTableMappingJson():
    tableMappingJson = {}
    allSchemaRule = initTableSelRule("395145838", "lambda-create-395145838", "%", "%", "include")
    mysqlExclueRule = initTableSelRule("395145839", "lambda-create-395145839", "mysql", "%", "exclude")
    sysExclueRule = initTableSelRule("395145840", "lambda-create-395145840", "sys", "%", "exclude")
    perfoExclueRule = initTableSelRule("395145841", "lambda-create-395145841", "performance_schema", "%", "exclude")
    infoExclueRule = initTableSelRule("395145842", "lambda-create-395145842", "information_schema", "%", "exclude")

    rules = [allSchemaRule,mysqlExclueRule,sysExclueRule,perfoExclueRule,infoExclueRule]
    tableMappingJson["rules"] = rules
    return tableMappingJson

# init Table selection Rule
def initTableSelRule(ruleId: str, ruleName: str, schema: str, tableName: str, ruleAction: str):
    schemaRule = {}
    schemaRule['rule-type'] = "selection"
    schemaRule['rule-id'] = ruleId
    schemaRule['rule-name'] = ruleName
    ruleObject = {
            "schema-name": schema,
            "table-name": tableName
    }
    schemaRule['object-locator'] = ruleObject
    schemaRule['rule-action'] = ruleAction
    schemaRule['filters'] = []
    return schemaRule

# init task setting
def initReplicationTaskSettings():
    settingJson = {}
    errorBehavior = {}
    logging = {
        "EnableLogging": True,
        "CloudWatchLogGroup": None,
        "CloudWatchLogStream": None
        }
    errorBehavior['FailOnNoTablesCaptured'] = False
    settingJson['ErrorBehavior'] = errorBehavior
    settingJson['Logging'] = logging
    return settingJson

# test DMS connection to endpoint
def testDMSConnection(endpointArn, repInstanceArn, region):
    try:
        client = getClient('dms', region)
        res = client.test_connection(
                ReplicationInstanceArn=repInstanceArn,
                EndpointArn=endpointArn
        )
        if res['Connection']['Status'] == "testing":
            time.sleep(3)
            logger.warn("%s is testing", endpointArn)
            return testDMSConnection(endpointArn, repInstanceArn, region)
        else:
            return res
    except client.exceptions.InvalidResourceStateFault as ExState:
        logger.warn("%s test exception", endpointArn)
        logger.warn(ExState)
        time.sleep(6)
        return testDMSConnection(endpointArn, repInstanceArn, region)
    except Exception as e:
        logger.error(e)
        return None

# check cdc task status
def checkDMSCDCtaskStat(taskArn, region) -> str:
    client = getClient("dms", region)
    res = client.describe_replication_tasks(
    Filters=[
        {
            'Name': 'replication-task-arn',
            'Values': [
                taskArn,
            ]
        },
    ]
    )
    if res == None or len(res['ReplicationTasks']) == 0:
        logger.warn("Task not found: %s", taskArn)
        return None
    else:
    # get first task status
        status = res['ReplicationTasks'][0]['Status']
        # doing sth but not running replication
        if status[-3:] == "ing" and status!="running":
            return checkDMSCDCtaskStat(taskArn, region)
        else:
            return status

# valid cdc task init status equal to "Created"
def DMStaskStatInCreated(taskArn, region) -> bool:
    client = getClient("dms", region)
    res = client.describe_replication_tasks(
    Filters=[
        {
            'Name': 'replication-task-arn',
            'Values': [
                taskArn,
            ]
        },
    ]
    )
    if res == None or len(res['ReplicationTasks']) == 0:
        logger.warn("Task not found: %s", taskArn)
        return False
    else:
    # get first task status
        status = res['ReplicationTasks'][0]['Status']
        # creating or modifing
        if status == "creating" or status[-3:]=="ing":
            time.sleep(10)
            return DMStaskStatInCreated(taskArn, region)
        elif status == "ready" or status == "created":
            return True
        else:
            return False

#  delete blue green and confirm read only of Aurora2 =OFF
def deleteBlueGreenDeploymentAndConfirm(client, deploymentIdentifier: str, blueclusterEndpoint: str, user_name, password):
    deployment = deleteBlueGreenDeployment(client, deploymentIdentifier)
    logging.warn("bule green deployment %s status: %s" % (deploymentIdentifier, deployment['Status']))
    time.sleep(10)
    readOnly = 'ON'
    conn = getConnection(blueclusterEndpoint, user_name, password)
    if conn is not None:
        logger.info("SUCCESS: Connection to RDS for Aurora2 instance succeeded")
    else:
        return readOnly
    # wait mysql5.7 to disable read only
    for i in range(100):
        time.sleep(3)
        cur = conn.cursor()
        # check if read_only = OFF
        cur.execute("show variables like 'read_only'")
        for row in cur:
            varName = row[0]
            readOnly = row[1]
        if readOnly == 'ON':
            logger.warn("The status of read_only is ON, to next loop")
            cur.close()
            continue
        # read_only changed to OFF, begin config rollback replication from Aurora3(src) to Aurora2(dest)
        elif readOnly == 'OFF':
            cur.close()
            break
    conn.close()
    return readOnly

def lambda_handler(event, context):
    """
    This function creates a new RDS database table and writes records to it
    """
    logger.info(event)
    detail = event['detail']
    # data = json.loads(detail)
    region = event['region']
    eventID = detail['EventID']
    sourceType = detail['SourceType']
    # blue-green-deployment id
    deploymentIdentifier = detail['SourceIdentifier']
    msg = detail['Message']
    if eventID != 'RDS-EVENT-0248':
        logging.info("Not Switchover completed on blue/green deployment. EventID: %s, msg: %s" % (eventID, msg))
        return "Not Switchover completed on blue/green deployment. EventID: %s, msg: %s" % (eventID, msg) 
    
    client = getClient('rds', region)
    blueGreenDeployment = descBlueGreenDeployment(client, deploymentIdentifier)
    SourceArn = blueGreenDeployment['Source']
    TargetArn = blueGreenDeployment['Target']
    SwitchoverDetails = blueGreenDeployment['SwitchoverDetails']
    #Status = SwitchoverDetail['Status']
    bluecluster = getCluster(client, SourceArn)
    logging.warn(bluecluster)
    source_rds_port = bluecluster['Port']
    blueclusterEndpoint = bluecluster['Endpoint']
    blueDBClusterIdentifier = bluecluster['DBClusterIdentifier']
    greencluster = getCluster(client, TargetArn)
    #logging.warn("greencluster: %s" % greencluster)
    greenclusterEndpoint = greencluster['Endpoint']
    greenDBClusterIdentifier = greencluster['DBClusterIdentifier']
    
    # after switchover, clusterId is greenDBClusterIdentifier
    if greenDBClusterIdentifier != clusterId  or region != clusterRegion:
        logging.error("Now Cluster %s Switchover completed, is not the one you input %s or not the region %s" % (blueDBClusterIdentifier, clusterId, clusterRegion))
        return "Now Cluster %s Switchover completed, is not the one you input %s or not the region %s" % (blueDBClusterIdentifier, clusterId, clusterRegion)
    logging.info("Found EventID: %s, msg: %s" % (eventID, msg))
    endTime = event['time']
    startTime = blueGreenDeployment['CreateTime']
    # get file and postition from event
    afterTargetClusterMembers = greencluster['DBClusterMembers']
    eventInstanceId = None
    #find the new writer
    for i in range(len(afterTargetClusterMembers)):
        instance = afterTargetClusterMembers[i]
        if instance['IsClusterWriter']:
            eventInstanceId = instance['DBInstanceIdentifier']
            break
    if eventInstanceId is None:
        logging.error("No Writer instance found in Cluster %s  after Switchover completed" % greenDBClusterIdentifier)
        return "No Writer instance found in Cluster %s  after Switchover completed" % greenDBClusterIdentifier
    # List all RDS events for new writer
    sourceType='db-instance'
    eventCategory = 'notification'
    #instanceWriterEvents = ListEvents(client, sourceType, eventInstanceId, startTime, endTime, eventCategory)
    instanceWriterEvents = ListEventsWithoutId(client, sourceType, startTime, endTime, eventCategory)
    logging.info("instanceWriterEvents: "+ str(instanceWriterEvents))
    if instanceWriterEvents is None or len(instanceWriterEvents) ==0:
        logging.error("No Events of instance %s found during Switchover, please check the Events from AWS RDS console" % eventInstanceId)
        return "No Events of instance %s found during Switchover" % eventInstanceId
    binlogMsg = binlog_msg if binlog_msg is not None else None
    # find binlog detail msg from events of new writer
    for i in range(len(instanceWriterEvents)):
        event = instanceWriterEvents[i]
        if event['Message'].startswith('Binary log coordinates in green environment after switchover' ):
            # add filter to match SourceIdentifier of event with clusterId
            # strip "-cluster" from "aurora2-db-cluster"
            clusterIdlike = clusterId[:-8] if (len(clusterId) > 8 and "-cluster" in clusterId) else clusterId
            instanceNameLike = ("%s-instance" % clusterIdlike)
            instanceNameLike2 = ("%s-green" % clusterIdlike)
            instanceNameLike3 = clusterIdlike
            if event['SourceIdentifier'].startswith(instanceNameLike) or event['SourceIdentifier'].startswith(instanceNameLike2) or event['SourceIdentifier'].startswith(instanceNameLike3):
                binlogMsg = event['Message']
                break
    if binlogMsg is None:
        logging.error("No Binary log coordinates of instance %s found during Switchover, pls make sure binlog of green has been enabled" % eventInstanceId)
        return "No Binary log coordinates of instance %s found during Switchover" % eventInstanceId
    logging.info("Instance %s 's binlogMsg: %s" % (eventInstanceId, binlogMsg))
    master_binlog_file, master_binlog_pos = getBinlogFilePos(binlogMsg)
    logging.warn("binlog: %s, %d" % (master_binlog_file, master_binlog_pos))
    
    # then config replication for rollback
    if cdc_method == 'MY_REPLICATION':
        config_master_sql_string = f"CALL mysql.rds_set_external_master('{greenclusterEndpoint}', {source_rds_port}, '{repl_user}', '{repl_pass}', '{master_binlog_file}', {master_binlog_pos}, false)"
        logger.info("config_master_sql_string: %s" % config_master_sql_string)
        # delete blue green deployment 
        readOnly = deleteBlueGreenDeploymentAndConfirm(client, deploymentIdentifier, blueclusterEndpoint, user_name, password)
        if readOnly == "ON":
            logger.error("Aurora2: %s can't write, pls check", blueDBClusterIdentifier)
            return
        elif readOnly == 'OFF':
            conn = getConnection(blueclusterEndpoint, user_name, password)
            cur = cur = conn.cursor()
            cur.execute(config_master_sql_string)
            cur.execute(start_slave_sql)
            # wait for replication connecting
            time.sleep(3)
            cur.execute("show slave status")
            logger.info("The status of slave replication is as following:")
            for row in cur:
                logger.info(row)
                break
    elif cdc_method == 'DMS_CDC':
        logger.info("You choose dms for cdc task")
        logger.info("Begin to config dms cdc task")
        # create DMS task with exists DMS replication instance
        dmsclient = getClient('dms', region)
        srcEndpointJson = MySQLSettingsJson(greenclusterEndpoint, source_rds_port, repl_user, repl_pass)
        destEndpointJson = MySQLSettingsJson(blueclusterEndpoint, source_rds_port, user_name, password)
        # create endpoint
        try:
            srcResponse = dmsclient.create_endpoint(EndpointIdentifier='src-green-' + greenDBClusterIdentifier,
                EndpointType='source',
                EngineName='aurora',
                SslMode='none',
                MySQLSettings=srcEndpointJson)
            srcEndpointArn = srcResponse['Endpoint']['EndpointArn']
            logger.info("Source Endpoint created successfully: %s", srcEndpointArn)
            destResponse = dmsclient.create_endpoint(EndpointIdentifier='dest-blue-' + blueDBClusterIdentifier,
                EndpointType='target',
                EngineName='aurora',
                SslMode='none',
                MySQLSettings=destEndpointJson)
            destEndpointArn = destResponse['Endpoint']['EndpointArn']
            logger.info("Target Endpoint created successfully: %s", destEndpointArn)
        except dmsclient.exceptions.ResourceAlreadyExistsFault as ex:
            logger.warn("Endpoint exists in DMS, pls check as following")
            logger.error(ex)
            return
        # test the connection of endpoints, no test
        #testSrcResponse = testDMSConnection(srcEndpointArn, repInstanceArn, region)
        #testDestResponse = testDMSConnection(destEndpointArn, repInstanceArn, region)
        #"successful","testing","failed","deleting"
        #if testSrcResponse['Connection']['Status'] == 'successful' and testDestResponse['Connection']['Status'] == 'successful':
        if 'successful' == 'successful':
            # create cdc task
            try:
                logger.info("Begin to create cdc task")
                tableMappingJson = initTableMappingJson()
                cdcResponse = dmsclient.create_replication_task(ReplicationTaskIdentifier='cdc-after-bg-switch-'+ clusterId,
                    SourceEndpointArn=srcEndpointArn,
                    TargetEndpointArn=destEndpointArn,
                    ReplicationInstanceArn=repInstanceArn,
                    MigrationType='cdc',
                    CdcStartPosition="%s:%d" % (master_binlog_file, master_binlog_pos),
                    TableMappings=json.dumps(tableMappingJson),
                    ReplicationTaskSettings=json.dumps(initReplicationTaskSettings())
                )
                cdcArn = cdcResponse['ReplicationTask']['ReplicationTaskArn'] if cdcResponse != None else None
                if cdcArn is None:
                    logger.error("CDC task of %s creatd failed after blue/green switch", clusterId)
                    return
                # delete blue green
                readOnly = deleteBlueGreenDeploymentAndConfirm(client, deploymentIdentifier, blueclusterEndpoint, user_name, password)
                if readOnly == "ON":
                    logger.error("Aurora2: %s can't write, pls check!!", blueDBClusterIdentifier)
                    return
                # check cdc task's status is created
                canStart = DMStaskStatInCreated(cdcArn, region)
                if not canStart:
                    logger.error("DMS replication task: %s for RDS/Aurora MySQL %s Created but not Started! Pls start it manually!!" % (taskName, clusterId))
                    return
                # start cdc task
                logger.info("Begin to start cdc task: %s", cdcArn)
                startRes = dmsclient.start_replication_task(
                    ReplicationTaskArn=cdcArn,
                    StartReplicationTaskType='start-replication'
                )
                # check cdc status
                repTask = startRes["ReplicationTask"] if startRes != None else None
                taskStatus = repTask['Status'] if repTask != None else None
                taskName = repTask['ReplicationTaskIdentifier'] if repTask != None else None
                if taskStatus != "running":
                    taskStatus = checkDMSCDCtaskStat(cdcArn, region)
                    logger.warn("Status of task %s is %s." % (taskName, taskStatus))
                    if taskStatus == "running":
                        logger.info("SUCCESS: set DMS replication task: %s for RDS/Aurora MySQL %s succeeded, task is running normally." % (taskName, clusterId))
                    else:
                        logger.error("DMS replication task: %s for RDS/Aurora MySQL %s Created but not running!!! Pls check it!!!" % (taskName, clusterId))
            except Exception as e:
                logger.error(e)
                return
        else:
            logger.error("Source Endpoint connected: %s, Target Endpoint connected: %s, pls check the Security Groups and VPC of replication instance and DB instances", (testSrcResponse['Connection']['Status'], testDestResponse['Connection']['Status']))
            return

    conn.close()
    logger.info("SUCCESS: set rollback replication for RDS/Aurora MySQL %s succeeded" % clusterId)
    return
