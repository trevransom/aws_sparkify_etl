import pandas as pd
import boto3
import json
import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# Define our core AWS credentials
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER = config.get("CLUSTER", "DB_USER")
DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DWH_PORT = config.get("CLUSTER", "DB_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# Define IAM and Redshift clients
iam = boto3.client(
    'iam',
    region_name='us-west-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

redshift = boto3.client(
    'redshift',
    region_name='us-west-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)


def setup_iam_roles():
    """
    - Here we are going to setup an IAM role
    - We will attach a policy and display the IAM ARN as well
    """
    # Create the IAM role
    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description='Allows Redshift Cluster to call AWS services on my behalf',
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [{
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal':
                            {'Service': 'redshift.amazonaws.com'}}],
                        'Version': '2012-10-17'
                }
            )
        )

    except Exception as e:
        print(e)

    # Attach Policy
    try:
        print('1.2 Attaching Policy')
        dwhRole = iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    except Exception as e:
        print(e)

    # Get and print the IAM role ARN
    print('1.3 Get the IAM role ARN')
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)


def create_redshift_cluster():
    """
    - Here we are going to programatically launch a redshift cluster
    - We will specify the options we want for the cluster
    """
    try:
        response = redshift.create_cluster(
            # Add parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Add parameters for identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Add parameter for role (to allow s3 access)
            IamRoles=[roleArn]
        )

    except Exception as e:
        print(e)


def display_redshift_properties(props):
    """
    - Set up a pandas dataframe to display helpful analytics about the redshift cluster
    """
    pd.DataFrame({
        "Param":
            ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
        "Value":
            [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
    })

    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]

    return pd.DataFrame(data=x, columns=["Key", "Value"])


def delete_redshift_cluster():
    """
    - Delete the redshift cluster
    """
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)


# setup_iam_roles()
# create_redshift_cluster()
# delete_redshift_cluster()

# Display information about our current redshift cluster
current_cluster_properties = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print(display_redshift_properties(current_cluster_properties))
