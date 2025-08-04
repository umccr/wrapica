#!/usr/bin/env python3

# import pytest
# from mockito import when
#
# from pathlib import Path
#
# from wrapica.project_data import get_project_data_id_from_project_id_and_path
#
# from uuid import uuid4
# from random import randint


# Mock up a ProjectDataPagedList object
# def generate_random_hex():
#     return hex(randint(0, 15)).lstrip("0x")
#
#
# def generate_random_file_id():
#     return "fil.%s" % "".join(
#         map(
#             lambda _: generate_random_hex(),
#             range(10)
#         )
#     )
#
#
# MOCK_PROJECT_ID = str(uuid4())
# MOCK_DATA_IDS = list(
#     map(
#         lambda iter_: generate_random_file_id(),
#         range(10)
#     )
# )
#
#
# def mock_project_data(*args, **kwargs):
#
#     return ProjectDataPagedList(
#         items=[
#             ProjectData(
#                 project_id=MOCK_PROJECT_ID,
#                 data=Data(
#                     id=MOCK_DATA_IDS[0],
#                     details=DataDetails(
#                         path="/dummy/path"
#                     )
#                 )
#             )
#         ]
#     )
#
#
# class TestGetProjectFileIdFromProjectIdAndPath:
#     project_id = MOCK_PROJECT_ID
#     data_id = MOCK_DATA_IDS[0]
#     data_path = Path("/dummy/path")
#
#     def test_get_project_file_id_from_project_id_and_path(self, monkeypatch):
#
#         monkeypatch.setattr(ProjectDataApi, "get_project_data_list", mock_project_data)
#
#         data_id = get_project_file_id_from_project_id_and_path(self.project_id, self.data_path)
#
#         assert data_id == self.data_id
