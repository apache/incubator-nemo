#!/usr/bin/env python3
#
# Copyright (C) 2018 Seoul National University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from flask import request, jsonify
from nemowebserver import app
from nemowebserver.models import *
from nemowebserver.database import db_session


def running_dags_to_json(running_dags):
    result = list()
    for running_dag in running_dags:
        result_element = dict()
        result_element["application_name"] = running_dag.application_name
        result_element["dag_type"] = running_dag.dag_type
        result_element["dag_json"] = running_dag.dag_json
        result.append(result_element)
    return jsonify(result)


def query_dag(application_name=None, dag_type=None):
    all_rows = db_session.query(RunningDAG.application_name, RunningDAG.dag_type, RunningDAG.dag_json)
    if application_name is None:
        return running_dags_to_json(all_rows)
    elif dag_type is None:
        return running_dags_to_json(all_rows.filter(RunningDAG.application_name == application_name))
    else:
        return running_dags_to_json(all_rows
                                    .filter(RunningDAG.application_name == application_name)
                                    .filter(RunningDAG.dag_type == dag_type))


@app.route("/", methods=['GET', 'POST'])
def all_graphs():
    if request.method == 'POST':
        json_content = request.get_json()
        new_running_dag = RunningDAG(
            json_content.get('application_id'),
            json_content.get('dag_type'),
            json_content.get('dag_json')
        )
        db_session.add(new_running_dag)
        db_session.commit()
        return jsonify({"success": True}), 200

    if request.method == 'GET':
        return query_dag(), 200


@app.route("/application/<application_name>", methods=['GET'])
def application_graphs(application_name):
    return query_dag(application_name), 200


@app.route("/application/<application_name>/<dag_type>", methods=['GET'])
def application_type_graph(application_name, dag_type):
    return query_dag(application_name, dag_type), 200
