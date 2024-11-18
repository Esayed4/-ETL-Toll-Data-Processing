from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args={
    'owner':'esayed',
    'start_date':days_ago(0),
    'email':'nabilesayed@gmail.com',
    'email_on_failure':True,
    'email_on_retry':True,
    'retry':1,
    'retry_delay':timedelta(minutes=5),
}

dag=DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)


destination_path = '/home/project/airflow/dags/finalassignment/staging'

unzip_data=BashOperator(
    task_id='unzip_data',
    bash_command="tar -xzvf $destination_path/tolldata.tgz -C \
    $destination_path",
    dag=dag,
)
extract_data_from_csv=BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d',' -f1-4 '/home/project/airflow/dags/finalassignment/vehicle-data.csv' \
    > '/home/project/airflow/dags/finalassignment/staging/csv_data.csv'",
    dag=dag,
)

extract_data_from_tsv=BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cat '/home/project/airflow/dags/finalassignment/tollplaza-data.tsv'\
     | tr '\t' ',' |    cut -d',' -f5-7 > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv",
    dag=dag,
)


extract_data_from_fixed_width=BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="cut -c59- /home/project/airflow/dags/finalassignment/payment-data.txt |  tr ' ' ',' > \
    '/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv'",
    dag=dag,
)

consolidate_data=BashOperator(
    task_id='consolidate_data',
    bash_command="paste  /home/project/airflow/dags/finalassignment/staging/csv_data.csv\
     /home/project/airflow/dags/finalassignment/staging/tsv_data.csv  \
     /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv > '/home/project/airflow/dags/finalassignment/staging/extracted_data.csv'",
    dag=dag,
)

transform_data=BashOperator(
    task_id='transform_data',
    bash_command="cut -d',' -f4 '/home/project/airflow/dags/finalassignment/staging/extracted_data.csv' | tr ['a-z'] ['A-Z'] > '/home/project/airflow/dags/finalassignment/staging/transformed_data.csv'",
    dag=dag,
)
unzip_data>>
[extract_data_from_csv,extract_data_from_tsv,extract_data_from_fixed_width]\
>>consolidate_data>>transform_data

