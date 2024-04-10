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

def getRdsClient(srvName, region_name):
    client = boto3.client('rds', region_name)
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
    
    client = getRdsClient('rds', region)
    blueGreenDeployment = descBlueGreenDeployment(client, deploymentIdentifier)
    SourceArn = blueGreenDeployment['Source']
    TargetArn = blueGreenDeployment['Target']
    SwitchoverDetails = blueGreenDeployment['SwitchoverDetails']
    #Status = SwitchoverDetail['Status']
    bluecluster = getCluster(client, SourceArn)
    logging.warn(bluecluster)
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
    binlogMsg = None
    # find binlog detail msg from events of new writer
    for i in range(len(instanceWriterEvents)):
        event = instanceWriterEvents[i]
        if event['Message'].startswith('Binary log coordinates in green environment after switchover' ):
            # add filter to match SourceIdentifier of event with clusterId
            instanceNameLike = ("%s-instance" % clusterId)
            if event['SourceIdentifier'].startswith(instanceNameLike):
                binlogMsg = event['Message']
                break
    if binlogMsg is None:
        logging.error("No Binary log coordinates of instance %s found during Switchover, pls make sure binlog of green has been enabled" % eventInstanceId)
        return "No Binary log coordinates of instance %s found during Switchover" % eventInstanceId
    logging.info("Instance %s 's binlogMsg: %s" % (eventInstanceId, binlogMsg))
    master_binlog_file, master_binlog_pos = getBinlogFilePos(binlogMsg)
    logging.warn("binlog: %s, %d" % (master_binlog_file, master_binlog_pos))
    # delete blue green deployment 
    deployment = deleteBlueGreenDeployment(client, deploymentIdentifier)
    logging.warn("bule green deployment %s status: %s" % (deploymentIdentifier, deployment['Status']))
    time.sleep(10)
    # then config replication for rollback
    config_master_sql_string = f"CALL mysql.rds_set_external_master('{greenclusterEndpoint}', {source_rds_port}, '{repl_user}', '{repl_pass}', '{master_binlog_file}', {master_binlog_pos}, false)"
    logger.info("config_master_sql_string: %s" % config_master_sql_string)
    conn = getConnection(blueclusterEndpoint, user_name, password)
    if conn is not None:
        logger.info("SUCCESS: Connection to RDS for MySQL instance succeeded")
    # wait mysql5.7 to disable read only
    for i in range(100):
        time.sleep(3)
        cur = conn.cursor()
        # check if read_only = OFF
        cur.execute("show variables like 'read_only'")
        varValue = 'ON'
        for row in cur:
            varName = row[0]
            varValue = row[1]
        if varValue == 'ON':
            logger.warn("The status of read_only is ON, to next loop")
            cur.close()
            continue
        elif varValue == 'OFF':
            cur.execute(config_master_sql_string)
            cur.execute(start_slave_sql)
            # wait for replication connecting
            time.sleep(3)
            cur.execute("show slave status")
            logger.info("The status of slave replication is as following:")
            for row in cur:
                logger.info(row)
            break
    conn.close()
    logger.info("SUCCESS: set rollback replication for RDS/Aurora MySQL %s succeeded" % clusterId)
    return "Added replication successfully for RDS/Aurora MySQL %s" % clusterId
    
