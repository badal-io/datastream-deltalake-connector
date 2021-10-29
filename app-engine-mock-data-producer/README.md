
# To run locally:
download and install the cloud_sql_proxy and start it using a Unix Socket https://cloud.google.com/sql/docs/mysql/connect-admin-proxy#start-proxy.

virtualenv --python python3 env
source env/bin/activate

Install depndancies:  'pip install -e .'

Set env vars
export MYSQL_DB=demo
export MYSQL_USER=root 
export MYSQL_PASS=<password>
export CLOUD_SQL_CONNECTION_NAME=<instance_name>

run : python app.py