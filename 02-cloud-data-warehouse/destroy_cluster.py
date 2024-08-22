import os
import boto3
import time
import configparser
from dotenv import load_dotenv
from utils import pretty_redshift_props
from logging_config import logging

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

# Initialize Boto3 clients
ec2 = boto3.resource('ec2', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
iam = boto3.client('iam', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
redshift = boto3.client('redshift', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)


def delete_redshift_cluster():
    """Delete the Redshift cluster if it exists."""
    try:
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
        logging.info(f"Deleting Redshift cluster {DWH_CLUSTER_IDENTIFIER}...")
        
        while True:
            try:
                myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
                logging.info("Current cluster status: Available")
                time.sleep(30)  # Wait 30 seconds before checking again
            except redshift.exceptions.ClusterNotFoundFault:
                logging.info(pretty_redshift_props(myClusterProps))
                logging.info(f"Redshift cluster {DWH_CLUSTER_IDENTIFIER} deleted successfully.")
                break
    except Exception as e:
        logging.error(f"Error deleting Redshift cluster: {e}")        

def delete_iam_role():
    """Detach the policy, delete the IAM role, and remove DWH_ENDPOINT and DWH_ROLE_ARN from dwh.cfg."""
    try:
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
        logging.info(f"IAM role {DWH_IAM_ROLE_NAME} deleted successfully.")

        # Remove DWH_ENDPOINT and DWH_ROLE_ARN from the configuration file
        config.remove_option('DWH', 'DWH_ENDPOINT')
        config.remove_option('DWH', 'DWH_ROLE_ARN')

        # Write the updated configuration back to the file
        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)
        
        logging.info("DWH_ENDPOINT and DWH_ROLE_ARN removed from dwh.cfg.")

    except iam.exceptions.NoSuchEntityException:
        logging.warning(f"IAM role {DWH_IAM_ROLE_NAME} does not exist or has already been deleted.")
    except Exception as e:
        logging.error(f"Error deleting IAM role or updating dwh.cfg: {e}")

def main():
    """Main function to delete the Redshift cluster, security group, and IAM role."""
    delete_redshift_cluster()
    delete_iam_role()

if __name__ == "__main__":
    main()
