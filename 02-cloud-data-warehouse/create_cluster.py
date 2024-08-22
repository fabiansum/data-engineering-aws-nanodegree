import os
import pandas as pd
import boto3
import json
from dotenv import load_dotenv
import configparser
import time
from logging_config import logging
from utils import pretty_redshift_props

# Load environment variables from .env file
load_dotenv()

# Read configuration from dwh.cfg
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = os.getenv('KEY')
SECRET = os.getenv('SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# Display configuration parameters
print(pd.DataFrame({
    "Param": [
        "DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", 
        "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", 
        "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"
    ],
    "Value": [
        DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, 
        DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, 
        DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME
    ]
}))

# Initialize Boto3 clients
ec2 = boto3.resource('ec2', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
s3 = boto3.resource('s3', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
iam = boto3.client('iam', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
redshift = boto3.client('redshift', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

# Create IAM Role
def create_iam_role():
    """Create an IAM role for Redshift with the necessary permissions."""
    try:
        logging.info("Creating a new IAM Role.")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'redshift.amazonaws.com'
                    }
                }],
                'Version': '2012-10-17'
            })
        )
    except Exception as e:
        logging.error(f"Error creating IAM Role: {e}")
        raise e
        
    logging.info('Attaching Policy.')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']

    logging.info('Getting the IAM role ARN.')
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    return roleArn

# Create Redshift Cluster
def create_redshift_cluster(roleArn):
    """Create a Redshift cluster using the specified IAM role."""
    try:
        logging.info("Creating Redshift cluster.")
        response = redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[roleArn]
        )
    except Exception as e:
        logging.error(f"Error creating Redshift cluster: {e}")
        raise e

# Authorize inbound traffic to the Redshift cluster
def authorize_security_group_ingress(vpcId):
    """Authorize inbound traffic to the Redshift cluster security group."""
    try:
        logging.info("Authorizing inbound traffic to the Redshift cluster.")
        vpc = ec2.Vpc(id=vpcId)
        defaultSg = list(vpc.security_groups.all())[0]
        logging.info(f"Default security group: {defaultSg}")
        
        try:
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(DWH_PORT),
                ToPort=int(DWH_PORT)
            )
            logging.info(f"Inbound traffic authorized on security group {defaultSg.id}.")
        except Exception as e:
            if 'InvalidPermission.Duplicate' in str(e):
                logging.warning(f"Security group rule already exists. Skipping authorization.")
            else:
                logging.error(f"Error authorizing security group ingress: {e}")
                raise e
    except Exception as e:
        logging.error(f"Error retrieving security group: {e}")
        raise e

def wait_for_cluster_creation(redshift, cluster_identifier):
    """Wait for the Redshift cluster to become available."""
    logging.info("Waiting for Redshift cluster to become available...")
    while True:
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
            cluster_status = myClusterProps['ClusterStatus']
            logging.info(f"Current cluster status: {cluster_status}")
            
            if cluster_status == 'available':
                logging.info("Cluster is now available.")
                print(pretty_redshift_props(myClusterProps))
                return myClusterProps
            else:
                time.sleep(30)  # Wait for 30 seconds before checking again

        except Exception as e:
            logging.error(f"Error checking cluster status: {e}")
            time.sleep(30)  # Wait for 30 seconds before retrying

def main():
    """Main function to create IAM role, Redshift cluster, and configure security group."""
    roleArn = create_iam_role()
    create_redshift_cluster(roleArn)

    myClusterProps = wait_for_cluster_creation(redshift, DWH_CLUSTER_IDENTIFIER)

    if myClusterProps['ClusterStatus'] == 'available':
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        print(f"DWH_ENDPOINT: {DWH_ENDPOINT}")
        print(f"DWH_ROLE_ARN: {DWH_ROLE_ARN}")
    
        # Update dwh.cfg
        config.set("DWH", "DWH_ENDPOINT", DWH_ENDPOINT)
        config.set("DWH", "DWH_ROLE_ARN", DWH_ROLE_ARN)
        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)

        authorize_security_group_ingress(myClusterProps['VpcId'])

if __name__ == "__main__":
    main()
