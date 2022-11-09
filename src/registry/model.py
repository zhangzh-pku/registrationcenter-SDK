import json
from typing import Dict, List, Union

import requests
from dflow import S3Artifact, upload_artifact

from .artifacts import Artifact, GitArtifact, HTTPArtifact, LocalPath

test_domain = "http://registration-center.test.dp.tech"


def obj_to_dict(obj):
    if isinstance(obj, Artifact):
        return obj.to_dict()
    elif isinstance(obj, S3Artifact):
        return {"s3": obj.to_dict()}
    elif isinstance(obj, Model):
        return {"model": {"id": obj.id}}
    elif isinstance(obj, Dataset):
        return {"dataset": {"id": obj.id}}


def obj_from_dict(d):
    if "model" in d:
        return Model.query(id=d["model"]["id"])
    if "dataset" in d:
        return Dataset.query(id=d["dataset"]["id"])
    else:
        return Artifact.from_dict(d)


class Model:

    def __init__(self,
                 namespace: str,
                 name: str,
                 version: str,
                 description: str = None,
                 readme: str = None,
                 author: str = None,
                 labels: Dict[str, str] = None,
                 status: str = None,
                 size: int = None,
                 id: str = None,
                 location: Union[HTTPArtifact, LocalPath, S3Artifact,
                                 Dict[str, Union[LocalPath,
                                                 S3Artifact, HTTPArtifact]],
                                 List[Union[LocalPath, S3Artifact,
                                            HTTPArtifact]]] = None,
                 code: GitArtifact = None,
                 source: Union[HTTPArtifact, LocalPath, S3Artifact, "Dataset",
                               Dict[str, Union[HTTPArtifact, LocalPath,
                                               S3Artifact, "Dataset"]],
                               List[Union[HTTPArtifact, LocalPath,
                                          S3Artifact, "Dataset"]]] = None,
                 parameters: Union[dict, LocalPath] = None,
                 spec: Union[dict, LocalPath] = None,
                 resources: Union[HTTPArtifact, LocalPath, S3Artifact,
                                  "Dataset",
                                  Dict[str, Union[HTTPArtifact, LocalPath,
                                                  S3Artifact, "Dataset"]],
                                  List[Union[HTTPArtifact, LocalPath,
                                             S3Artifact, "Dataset"]]] = None,
                 **kwargs,
                 ) -> None:
        """
        Model

        Args:
            namespace: namespace
            name: name
            version: version
            description: short description
            readme: long description
            author: author
            labels: labels
            status: status
            size: artifact size
            location: storage location, either locally or remotely
            code: source code used for generating the model
            source: artifacts used for generating the model
            parameters: parameters used for generating the model
            spec: specification of the model
            resources: related artifacts of the model
        """
        self.namespace = namespace
        self.name = name
        self.description = description
        self.readme = readme
        self.author = author
        self.version = version
        self.labels = labels
        self.status = status
        self.size = size
        self.location = location
        self.code = code
        self.source = source
        self.parameters = parameters
        self.spec = spec
        self.resources = resources
        self.id = id

    def __repr__(self):
        return "<Model %s/%s:%s>" % (self.namespace, self.name, self.version)

    def to_dict(self):
        d = {}
        for key, value in self.__dict__.items():
            if key in ["location", "code", "source", "resources"]:
                if value is None:
                    d[key] = None
                elif isinstance(value, (Artifact, S3Artifact, Model, Dataset)):
                    d[key] = obj_to_dict(value)
                elif isinstance(value, dict):
                    d[key] = {"dict":
                              {k: obj_to_dict(v) for k, v in value.items()}}
                elif isinstance(value, list):
                    d[key] = {"list": [obj_to_dict(i) for i in value]}
                else:
                    raise TypeError("%s is not supported artifact"
                                    % type(value))
            else:
                d[key] = value
        return d

    @classmethod
    def from_dict(cls, d):
        kwargs = {}
        for key, value in d.items():
            if key in ["location", "code", "source", "resources"]:
                if not value:
                    kwargs[key] = None
                elif "dict" in value:
                    kwargs[key] = {k: obj_from_dict(v) for k, v in
                                   value["dict"].items()}
                elif "list" in value:
                    kwargs[key] = [obj_from_dict(i) for i in value["list"]]
                else:
                    kwargs[key] = obj_from_dict(value)
            else:
                kwargs[key] = value
        return cls(**kwargs)

    def handle_local_artifacts(self):
        for key in ["location", "code", "source", "resources"]:
            value = getattr(self, key)
            print(key, value)
            if isinstance(value, LocalPath):
                setattr(self, key, upload_artifact(value.path))
            elif isinstance(value, dict):
                for k, v in value.items():
                    if isinstance(v, LocalPath):
                        value[k] = upload_artifact(v.path)
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    if isinstance(v, LocalPath):
                        value[i] = upload_artifact(v.path)

    def insert(self,
               domain: str = test_domain):
        self.handle_local_artifacts()
        url = domain + "/api/v1/model"
        body = self.to_dict()
        if body["location"] is None:
            raise ValueError("Location of %s not provided" % self)
        r = requests.post(url=url, data=json.dumps(body))
        if r.status_code < 200 or r.status_code >= 300:
            print("got unexcept http status:", r.status_code)
            return
        body = r.json()
        if body.get("code", 1) != 0:
            print(body.get("error", "got error but no error set"))
            return
        data = body.get("data", {})
        self.id = data.get("id", "")

    @classmethod
    def query(cls,
              namespace: str = None,
              name: str = None,
              version: str = None,
              domain: str = test_domain,
              id: str = None) -> list:
        url = domain + "/api/v1/model"
        d = {"namespace": namespace, "name": name, "version": version,
             "id": id}
        r = requests.get(url=url, params=d)
        if r.status_code < 200 or r.status_code >= 300:
            print("got unexcept http status:", r.status_code)
            return
        d = r.json()
        lis = d.get("data", {}).get("models", [])
        if lis is None:
            return []
        res = []
        for mod in lis:
            m = cls.from_dict(mod)
            res.append(m)
        return res


class Dataset:

    def __init__(self,
                 namespace: str,
                 name: str,
                 version: str,
                 description: str = None,
                 readme: str = None,
                 author: str = None,
                 labels: Dict[str, str] = None,
                 status: str = None,
                 size: int = None,
                 id: str = None,
                 location: Union[HTTPArtifact, LocalPath, S3Artifact,
                                 Dict[str, Union[LocalPath,
                                                 S3Artifact, HTTPArtifact]],
                                 List[Union[LocalPath, S3Artifact,
                                            HTTPArtifact]]] = None,
                 code: GitArtifact = None,
                 source: Union[HTTPArtifact, LocalPath, S3Artifact, "Dataset",
                               Dict[str, Union[HTTPArtifact, LocalPath,
                                               S3Artifact, "Dataset"]],
                               List[Union[HTTPArtifact, LocalPath,
                                          S3Artifact, "Dataset"]]] = None,
                 parameters: Union[dict, LocalPath] = None,
                 spec: Union[dict, LocalPath] = None,
                 resources: Union[HTTPArtifact, LocalPath, S3Artifact,
                                  "Dataset",
                                  Dict[str, Union[HTTPArtifact, LocalPath,
                                                  S3Artifact, "Dataset"]],
                                  List[Union[HTTPArtifact, LocalPath,
                                             S3Artifact, "Dataset"]]] = None,
                 **kwargs,
                 ) -> None:
        self.namespace = namespace
        self.name = name
        self.description = description
        self.readme = readme
        self.author = author
        self.version = version
        self.labels = labels
        self.status = status
        self.size = size
        self.location = location
        self.code = code
        self.source = source
        self.parameters = parameters
        self.spec = spec
        self.resources = resources
        self.id = id

    def __repr__(self):
        return "<Dataset %s/%s:%s>" % (self.namespace, self.name, self.version)

    def to_dict(self):
        d = {}
        for key, value in self.__dict__.items():
            if key in ["location", "code", "source", "resources"]:
                if value is None:
                    d[key] = None
                elif isinstance(value, (Artifact, S3Artifact, Model, Dataset)):
                    d[key] = obj_to_dict(value)
                elif isinstance(value, dict):
                    d[key] = {"dict":
                              {k: obj_to_dict(v) for k, v in value.items()}}
                elif isinstance(value, list):
                    d[key] = {"list": [obj_to_dict(i) for i in value]}
                else:
                    raise TypeError("%s is not supported artifact"
                                    % type(value))
            else:
                d[key] = value
        return d

    @classmethod
    def from_dict(cls, d):
        kwargs = {}
        for key, value in d.items():
            if key in ["location", "code", "source", "resources"]:
                if not value:
                    kwargs[key] = None
                elif "dict" in value:
                    kwargs[key] = {k: obj_from_dict(v) for k, v in
                                   value["dict"].items()}
                elif "list" in value:
                    kwargs[key] = [obj_from_dict(i) for i in value["list"]]
                else:
                    kwargs[key] = obj_from_dict(value)
            else:
                kwargs[key] = value
        return cls(**kwargs)

    def handle_local_artifacts(self):
        for key in ["location", "code", "source", "resources"]:
            value = getattr(self, key)
            if isinstance(value, LocalPath):
                setattr(self, key, upload_artifact(value.path))
            elif isinstance(value, dict):
                for k, v in value.items():
                    if isinstance(v, LocalPath):
                        value[k] = upload_artifact(v.path)
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    if isinstance(v, LocalPath):
                        value[i] = upload_artifact(v.path)

    def insert(self,
               domain: str = test_domain):
        self.handle_local_artifacts()
        url = domain + "/api/v1/data"
        body = self.to_dict()
        if body["location"] is None:
            raise ValueError("Location of %s not provided" % self)
        r = requests.post(url=url, data=json.dumps(body))
        if r.status_code < 200 or r.status_code >= 300:
            print("got unexcept http status:", r.status_code)
            return
        body = r.json()
        if body.get("code", 1) != 0:
            print(body.get("error", "got error but no error set"))
            return
        data = body.get("data", {})
        self.id = data.get("id", "")

    @classmethod
    def query(cls,
              namespace: str = None,
              name: str = None,
              version: str = None,
              domain: str = test_domain,
              down_load: bool = False,
              id: int = None) -> list:
        url = domain + "/api/v1/data"
        d = {"namespace": namespace, "name": name, "version": version,
             "id": id}
        r = requests.get(url=url, params=d)
        if r.status_code < 200 or r.status_code >= 300:
            print("got unexcept http status:", r.status_code)
            return
        d = r.json()
        lis = d.get("data", {}).get("data", [])
        res = []
        for data in lis:
            d = cls.from_dict(data)
            res.append(d)
        return res


class Workflow:

    def __init__(self,
                 namespace: str,
                 name: str,
                 version: str,
                 description: str = None,
                 readme: str = None,
                 author: str = None,
                 labels: dict = None,
                 status: str = None,
                 code: dict = None,
                 python_package: str = None,
                 docker_image: str = None,
                 id: str = None) -> None:
        self.namespace = namespace
        self.name = name
        self.description = description
        self.readme = readme
        self.author = author
        self.version = version
        self.labels = labels
        self.status = status
        self.code = code
        self.python_package = python_package
        self.docker_image = docker_image
        self.id = id

    def insert(self,
               domain: str = test_domain,
               upload: bool = False):
        url = domain + "/api/v1/workflow"
        d = self.__dict__
        for k in d:
            if isinstance(d[k], LocalPath):
                self.__setattr__(k, upload_artifact(d[k].path))
        body = {
            key: d[key]
            for key in d if "__" not in key and key is not None
        }
        r = requests.post(url=url, data=json.dumps(body))
        if r.status_code < 200 or r.status_code >= 300:
            print("got unexcept http status:", r.status_code)
            return
        body = r.json()
        if body.get("code", 1) != 0:
            print(body.get("error", "got error but no error set"))
            return
        data = body.get("data", {})
        self.id = data.get("id", "")

    @classmethod
    def query(cls,
              namespace: str = None,
              name: str = None,
              version: str = None,
              domain: str = test_domain,
              id: str = None) -> list:
        url = domain + "/api/v1/workflow"
        d = {"namespace": namespace, "name": name, "version": version,
             "id": id}
        r = requests.get(url=url, params=d)
        if r.status_code < 200 or r.status_code >= 300:
            print("got unexcept http status:", r.status_code)
            return
        d = r.json()
        lis = d.get("data", {}).get("workflows", [])
        res = []
        for wf in lis:
            try:
                name = wf["name"]
                namespace = wf["namespace"]
                version = wf["version"]
            except KeyError:
                continue
            w = cls(namespace, name, version)
            for k in wf:
                w.__setattr__(k, wf[k])
            res.append(w)
        return res


class OP:

    def __init__(self,
                 namespace: str,
                 name: str,
                 version: str,
                 description: str = None,
                 readme: str = None,
                 author: str = None,
                 labels: dict = None,
                 status: str = None,
                 code: dict = None,
                 python_package: str = None,
                 docker_image: str = None,
                 inputs: dict = None,
                 outputs: dict = None,
                 execute: dict = None,
                 id: str = None) -> None:
        self.namespace = namespace
        self.name = name
        self.description = description
        self.readme = readme
        self.author = author
        self.version = version
        self.labels = labels
        self.status = status
        self.code = code
        self.python_package = python_package
        self.docker_image = docker_image
        self.inputs = inputs
        self.outputs = outputs
        self.execute = execute
        self.id = id

    def insert(self,
               domain: str = test_domain,
               upload: bool = False):
        url = domain + "/api/v1/OP"
        d = self.__dict__
        body = {
            key: d[key]
            for key in d if "__" not in key and key is not None
        }
        r = requests.post(url=url, data=json.dumps(body))
        if r.status_code < 200 or r.status_code >= 300:
            print("got unexcept http status:", r.status_code)
            return
        body = r.json()
        if body.get("code", 1) != 0:
            print(body.get("error", "got error but no error set"))
            return
        data = body.get("data", {})
        self.id = data.get("id", "")
        if upload:
            d = self.__dict__
            for k in d:
                if isinstance(d[k], LocalPath):
                    self.__setattr__(k, upload_artifact(d[k].path))

    @classmethod
    def query(cls,
              namespace: str = None,
              name: str = None,
              version: str = None,
              domain: str = test_domain,
              id: str = None) -> list:
        url = domain + "/api/v1/OP"
        d = {"namespace": namespace, "name": name, "version": version,
             "id": id}
        r = requests.get(url=url, params=d)
        if r.status_code < 200 or r.status_code >= 300:
            print("got unexcept http status:", r.status_code)
            return
        d = r.json()
        lis = d.get("data", {}).get("OPs", [])
        res = []
        for op in lis:
            try:
                name = op["name"]
                namespace = op["namespace"]
                version = op["version"]
            except KeyError:
                continue
            o = cls(namespace, name, version)
            for k in op:
                o.__setattr__(k, op[k])
            res.append(o)
        return res
