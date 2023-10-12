import datetime
import json
import logging
import sys
import traceback
import argparse

import apache_beam as beam
import pytz
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions, GoogleCloudOptions, WorkerOptions
from google.cloud import bigquery as bq
from google.cloud.exceptions import NotFound


def parse_json(data):
    parsed_data = json.loads(data)
    current_ts = datetime.datetime.now(tz=pytz.timezone('Australia/NSW'))
    str_current_ts = current_ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    parsed_data["load_dttm"] = str_current_ts
    return parsed_data


class CustomWriteToBigquery(beam.DoFn):
    def __init__(self, def_project, data_table, error_table):

        d_project = data_table.split(":")[0]
        d_dataset = data_table.split(":")[1].split(".")[0]
        d_table = data_table.split(":")[1].split(".")[1]

        if d_project and d_dataset and d_table:
            self.dest_table = bq.Table.from_string("{}.{}.{}".format(d_project, d_dataset, d_table))
        else:
            print("Table not found, {}.{}.{}".format(d_project, d_dataset, d_table))
            sys.exit(-1)

        self.error_table = error_table
        self.def_project = def_project
        self.rows = list()

    def start_bundle(self):
        self.rows = []

    def process(self, element, *args, **kwargs):
        self.rows.append(element)

    def finish_bundle(self):
        bq_c = bq.Client(project=self.def_project)
        errors = []
        errors = bq_c.insert_rows_json(
            table=self.dest_table,
            json_rows=self.rows,
            skip_invalid_rows=True
        )
        failed_rows = []

        current_ts = datetime.datetime.now(tz=pytz.timezone('Australia/NSW'))
        str_current_ts = current_ts.strftime("%Y-%m-%d %H:%M:%S UTC")

        if errors:
            for error in errors:
                failed_row = dict()
                failed_row['errors'] = error["errors"]
                failed_row['source_data'] = json.dumps(self.rows[error['index']])
                failed_row["load_dttm"] = str_current_ts
                failed_rows.append(failed_row)
            error_errors = bq_c.insert_rows_json(
                table=self.error_table,
                json_rows=failed_rows,
                skip_invalid_rows=True
            )
            if error_errors:
                print("Could not handle insert errors.\nPayload data:{}".format("\n".join(error_errors)))


def get_error_table(def_project_id, full_target_table):

    target_project = full_target_table.split(":")[0]
    target_dataset = full_target_table.split(":")[1].split(".")[0]
    target_table = full_target_table.split(":")[1].split(".")[1]

    bq_c = bq.Client(project=def_project_id)

    if target_project and target_dataset and target_table:
        target_error_table = "{}_errors".format(target_table)
    else:
        print("Invalid dest table id")
        sys.exit(-1)

    full_error_table_name = "{}.{}.{}".format(target_project, target_dataset, target_error_table)

    try:
        print("Table check: _data_errors checking")
        target_table_id = bq_c.get_table(
            bq.table.TableReference.from_string("{}.{}.{}".format(target_project, target_dataset, target_error_table)))
        print("Table check: _data_errors exists.")
    except NotFound:
        print("_errors table does not exist.{}.{}.{} will be created".format(target_project, target_dataset,
                                                                                    target_error_table))
        table_obj = bq.Table(
            table_ref=bq.table.TableReference.from_string("{}.{}.{}".format(
                target_project, target_dataset,
                target_error_table
            )),
            schema=[bq.SchemaField(
                name="errors",
                field_type="RECORD",
                fields=(
                    bq.SchemaField(name="reason", field_type="STRING"),
                    bq.SchemaField(name="location", field_type="STRING"),
                    bq.SchemaField(name="debugInfo", field_type="STRING"),
                    bq.SchemaField(name="message", field_type="STRING")
                ),
                mode="REPEATED"
            ),
                bq.SchemaField(
                    name="source_data",
                    field_type="STRING"
                ),
                bq.SchemaField(
                    name="load_dttm",
                    field_type="TIMESTAMP"
                )
            ]
        )
        bq_c.create_table(table_obj, exists_ok=True)

    except Exception as ex:
        print(traceback.print_exc())
    return full_error_table_name


def build_and_run_pipe_line(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_subscription',
        required=True,
        help='Input PubSub Subscriptions to read the messages'
    )
    parser.add_argument(
        '--dest_table_id',
        required=True,
        help='Target bigquery table must be in the format project_id:dataset_id.table_id'
    )

    parser.add_argument(
        '--subnetwork',
        required=False,
        help='Subnetwork'
    )


    parameters, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    project_id = pipeline_options.view_as(GoogleCloudOptions).project
    pipeline_options.view_as(WorkerOptions).subnetwork = parameters.subnetwork


    dest_error_table_id = get_error_table(project_id, parameters.dest_table_id)

    with beam.Pipeline(options=pipeline_options) as p:
        message = (
                p
                | "Read from PubSub subscription" >> beam.io.ReadFromPubSub(subscription=parameters.input_subscription)
                | "Decode message" >> beam.Map(lambda event: event.decode('utf-8'))
                | "Parse as JSON" >> beam.Map(parse_json)
        )

        _ = message | "Write into Bigquery" >> beam.ParDo(CustomWriteToBigquery(
            def_project=project_id,
            data_table=parameters.dest_table_id,
            error_table=dest_error_table_id
        ))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    build_and_run_pipe_line()