sudo su -
sudo su -
sudo su -
sudo su -
sudo su -
sudo su -
aws ec2 describe-instances --instance-ids i-075168bd4316d00e4 --query 'Reservations[*].Instances[*].PublicIpAddress' --output text --profile int-dev
aws ec2 describe-instances --instance-ids i-075168bd4316d00e4 --query 'Reservations[*].Instances[*].PublicIpAddress' --output text --profile int-dev
exit
sudo su -
cd ~
ls
cd bi_prime/
ls
source bin/activate
ls
sudo yum -y install https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
sudo yum install postgresql-server
sudo yum remove postgresql-server postgresql postgresql-libs
amazon-linux-extras list | grep postgres
deactivate
amazon-linux-extras list | grep postgres
sudo amazon-linux-extras enable postgresql14
lsudo yum clean metadata
sudo yum clean metadata
sudo yum install postgresql-server
sudo postgresql-setup --initdb
sudo systemctl start postgresql
sudo systemctl enable postgresql
sudo -i -u postgres
ls
cd ~
ls
cd airflow/
ls
cd ..
rm tar_extract.py 
ls
rm retail_etl.py 
rm bi_dag_reduced_output/
ls
rm -rf bi_dag_reduced_output/
ls
airflow db init
cd !
ls
cd bi_prime/
ls
source  bin/activate
airflow db init
pip install psycopg2-binary
how do i get my pg credentials
ls
cd ..
ls
cd airflow/
ls
sudo -u postgres psql
ls
nano airflow.cfg 
vim airflow.cfg 
vim airflow.cfg 
airflow db init
sudo  /var/lib/pgsql/data/
ls
cd //
ls
cd var/
ls
cd lib/
ls
cd pgsql/
sudo su
airflow db init
sudo systemctl restart postgresql
airflow db init
ls
exit
cd ~
ls
 cd bi_prime/
ls
source bin/activate
ls
cd /home
ls
cd ssm-user/
ls
cd airflow/
ls
vim airflow.cfg 
airflow db init
pwd
python --version
pip3 show airflow
pip3 show apache-airflow
pip install 'apache-airflow==2.6.3' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-3.7.txt"
pip3 install --upgrade pip
pip install 'apache-airflow==2.2.3' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-3.7.txt"
airflow db init
clear
airflow db init
cd /home/
l
ls
cd ssm-user/
ls
cd airflow/
ls
vim airflow.cfg 
echo $AIRFLOW_HOME
pwd
airflow db upgrade
export AIRFLOW_HOME=/home/ssm-user/airflow
echo $AIRFLOW_HOME
airflow db init
    f"error: sqlite C library version too old (< {min_sqlite_version}). "
airflow.exceptions.AirflowConfigException: error: sqlite C library version too old (< 3.15.0). See https://airflow.apache.org/docs/apache-airflow/2.2.3/howto/set-up-database.html#setting-up-a-sqlite-databas
    f"error: sqlite C library version too old (< {min_sqlite_version}). "
airflow.exceptions.AirflowConfigException: error: sqlite C library version too old (< 3.15.0). See https://airflow.apache.org/docs/apache-airflow/2.2.3/howto/set-up-database.html#setting-up-a-sqlite-databas
source ~/.bashrc
