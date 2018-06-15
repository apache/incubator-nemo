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
from sqlalchemy import Table, Column, Integer, String, Enum
from nemowebserver.database import Base


class RunningDAG(Base):
    __tablename__ = 'running_dag'
    id = Column(Integer, primary_key=True)
    application_name = Column(String(50))
    dag_type = Column(Enum("IR", "Logical", "Physical"))
    dag_json = Column(String(5000))

    def __init__(self, application_name, dag_type, dag_json):
        self.application_name = application_name
        self.dag_type = dag_type
        self.dag_json = dag_json

    def __repr__(self):
        return '<Application name: %r, Dag type: %r, Dag Json: %r>' \
               % (self.application_name, self.dag_type, self.dag_json)
