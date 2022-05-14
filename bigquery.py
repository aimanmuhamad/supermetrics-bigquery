import requests
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from urllib.parse import urlparse, urlencode, urlunparse, parse_qsl

# Construct a credentials object
credentials = service_account.Credentials.from_service_account_file(
    '<path of your service account>',
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Construct a BigQuery client object.
bigquery_client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id
)
dataset = bigquery.Dataset('<project-name>.<schema-name>')


def set_query_field(url, field, value, replace=False):
    # Parse out the different parts of the URL.
    components = urlparse(url)
    query_pairs = parse_qsl(urlparse(url).query)

    if replace:
        query_pairs = [(f, v) for (f, v) in query_pairs if f != field]
    query_pairs.append((field, value))

    new_query_str = urlencode(query_pairs)

    # Finally, construct the new URL
    new_components = (
        components.scheme,
        components.netloc,
        components.path,
        components.params,
        new_query_str,
        components.fragment
    )
    return urlunparse(new_components)


def upload_supermetrics_csv_result_to_bigquery(url, table_name, start_date, end_date):
    csv_name = table_name + "--" + start_date + ".csv"
    csv_path = "/usr/local/airflow/temp/" + csv_name

    csv_url = set_query_field(url, "start-date", start_date, True)
    csv_url = set_query_field(csv_url, "end-date", end_date, True)

    with requests.Session() as s:
        # Download CSV
        download = s.get(csv_url)
        # Save CSV To Temp. File
        open(csv_path, 'w+').write(download.text)

    # A terrible check to see if url actually contains data
    if download.text != 'Error: No data found':
        table_ref = dataset.table("sm_" + table_name)
        table = bigquery.Table(table_ref)

        try:
            table = bigquery_client.create_table(table)
        except:
            pass

        job_config = bigquery.LoadJobConfig()
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
        job_config.source_format = "CSV"

        with open(csv_path, 'rb') as source_file:
            job = bigquery_client.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config
            )

        job.result()
        os.remove(csv_path)