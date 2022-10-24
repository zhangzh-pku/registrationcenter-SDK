import json
import os
from typing import Dict, Union

import oss2
import requests

from .artifacts import (Artifact, GitArtifact, HTTPArtifact, LocalPath,
                        OSSArtifact, S3Artifact)


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
                 location: Union[HTTPArtifact, LocalPath,
                                 S3Artifact, OSSArtifact] = None,
                 code: GitArtifact = None,
                 source: Dict[str, Union[HTTPArtifact, LocalPath,
                                         S3Artifact, OSSArtifact,
                                         "Dataset"]] = None,
                 parameters: Union[dict, LocalPath] = None,
                 spec: Union[dict, LocalPath] = None,
                 resources: Dict[str, Union[HTTPArtifact, LocalPath,
                                            S3Artifact, OSSArtifact,
                                            "Dataset"]] = None,
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

    def __repr__(self):
        return "<Model %s/%s:%s>" % (self.namespace, self.name, self.version)

    def to_dict(self):
        d = {}
        for key, value in self.__dict__.items():
            if key in ["location", "code"]:
                if value is None:
                    d[key] = None
                elif isinstance(value, Artifact):
                    d[key] = value.to_dict()
                else:
                    raise TypeError("%s is not supported artifact"
                                    % type(value))
            elif key in ["source", "resources"]:
                if value is None:
                    d[key] = None
                elif isinstance(value, dict):
                    d2 = {}
                    for k, v in value.items():
                        if isinstance(v, Artifact):
                            d2[k] = v.to_dict()
                        elif isinstance(v, Dataset):
                            d2[k] = {"dataset": {"id": v.id}}
                        else:
                            raise TypeError("%s is not supported artifact"
                                            % type(v))
                    d[key] = d2
                else:
                    raise TypeError("%s must be a dict" % key)
            else:
                d[key] = value
        return d

    @classmethod
    def from_dict(cls, d):
        kwargs = {}
        for key, value in d.items():
            if key in ["location", "code"]:
                if value is None:
                    kwargs[key] = None
                else:
                    kwargs[key] = Artifact.from_dict(value)
            elif key in ["source", "resources"]:
                if value is None:
                    kwargs[key] = None
                else:
                    kwargs[key] = {k: Artifact.from_dict(v) for k, v in value.items()}
            else:
                kwargs[key] = value
        return cls(**kwargs)


    # 默认只保存路径到注册中心，oss为True时上传source、parameters、spec、resource到oss平台
    # 如果要上传到oss，那么必须要在系统环境变量中配置相关环境变量
    # 具体细节见使用文档
    def insert(self,
               domain: str = "http://127.0.0.1:8080",
               oss_flag: bool = False):
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
        if oss_flag:
            bucket = get_bucket()
            # path = [p for p in d if "_path" in p and p is not None]
            prefix = "registryCentre/model/"
            pre_path = prefix + self.namespace + "/" + self.name + "_" + self.version + "/"
            if self.source_path != None:
                src_path = pre_path + self.source_path
                oss2.resumable_upload(bucket, src_path, self.source_path)
            if self.spec_path != None:
                src_path = pre_path + self.spec_path
                oss2.resumable_upload(bucket, src_path, self.spec_path)
            if self.resource_path != None:
                src_path = pre_path + self.resource_path
                oss2.resumable_upload(bucket, src_path, self.resource_path)
            if self.parameters_path != None:
                src_path = pre_path + self.parameters_path
                oss2.resumable_upload(bucket, src_path, self.parameters_path)

    @classmethod
    def query(cls,
              namespace: str,
              name: str = None,
              version: str = None,
              domain: str = "http://127.0.0.1:8080",
              down_load: bool = False) -> list:
        url = domain + "/api/v1/model"
        d = {"namespace": namespace, "name": name, "version": version}
        r = requests.get(url=url, params=d)
        if r.status_code < 200 or r.status_code >= 300:
            print("got unexcept http status:", r.status_code)
            return
        d = r.json()
        lis = d.get("data", {}).get("models", [])
        res = []
        for mod in lis:
            m = cls.from_dict(mod)
            res.append(m)
        if down_load:
            bucket = get_bucket()
            prefix = "registryCentre/model/"
            for mod in res:
                pre_path = prefix + mod.namespace + "/" + mod.name + "_" + mod.version + "/"
                if mod.source_path != None and mod.source_path != "":
                    src_path = pre_path + mod.source_path
                    oss2.resumable_download(bucket, src_path,
                                            mod.source_path + "_download")
                if mod.spec_path != None and mod.spec_path != "":
                    src_path = pre_path + mod.spec_path
                    oss2.resumable_download(bucket, src_path,
                                            mod.spec_path + "_download")
                if mod.resource_path != None and mod.resource_path != "":
                    src_path = pre_path + mod.resource_path
                    oss2.resumable_download(bucket, src_path,
                                            mod.resource_path + "_download")
                if mod.parameters_path != None and mod.parameters_path != "":
                    src_path = pre_path + mod.parameters_path
                    oss2.resumable_download(bucket, src_path,
                                            mod.parameters_path + "_download")

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
                 location: Union[HTTPArtifact, LocalPath,
                                 S3Artifact, OSSArtifact] = None,
                 code: GitArtifact = None,
                 source: Dict[str, Union[HTTPArtifact, LocalPath,
                                         S3Artifact, OSSArtifact,
                                         Model, "Dataset"]] = None,
                 parameters: Union[dict, LocalPath] = None,
                 spec: Union[dict, LocalPath] = None,
                 resources: Dict[str, Union[HTTPArtifact, LocalPath,
                                            S3Artifact, OSSArtifact,
                                            Model, "Dataset"]] = None,
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

    def __repr__(self):
        return "<Dataset %s/%s:%s>" % (self.namespace, self.name, self.version)

    def to_dict(self):
        d = {}
        for key, value in self.__dict__.items():
            if key in ["location", "code"]:
                if value is None:
                    d[key] = None
                elif isinstance(value, Artifact):
                    d[key] = value.to_dict()
                else:
                    raise TypeError("%s is not supported artifact"
                                    % type(value))
            elif key in ["source", "resources"]:
                if value is None:
                    d[key] = None
                elif isinstance(value, dict):
                    d2 = {}
                    for k, v in value.items():
                        if isinstance(v, Artifact):
                            d2[k] = v.to_dict()
                        elif isinstance(v, Model):
                            d2[k] = {"model": {"id": v.id}}
                        elif isinstance(v, Dataset):
                            d2[k] = {"dataset": {"id": v.id}}
                        else:
                            raise TypeError("%s is not supported artifact"
                                            % type(v))
                    d[key] = d2
                else:
                    raise TypeError("%s must be a dict" % key)
            else:
                d[key] = value
        return d

    @classmethod
    def from_dict(cls, d):
        kwargs = {}
        for key, value in d.items():
            if key in ["location", "code"]:
                if value is None:
                    kwargs[key] = None
                else:
                    kwargs[key] = Artifact.from_dict(value)
            elif key in ["source", "resources"]:
                if value is None:
                    kwargs[key] = None
                else:
                    kwargs[key] = {k: Artifact.from_dict(v) for k, v in value.items()}
            else:
                kwargs[key] = value
        return cls(**kwargs)

    def insert(self,
               domain: str = "http://127.0.0.1:8080",
               oss_flag: bool = False):
        url = domain + "/api/v1/data"
        body = self.to_dict()
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
        if oss_flag:
            bucket = get_bucket()
            # path = [p for p in d if "_path" in p and p is not None]
            prefix = "registryCentre/data/"
            pre_path = prefix + self.namespace + "/" + self.name + "_" + self.version + "/"
            if self.source_path != None:
                src_path = pre_path + self.source_path
                oss2.resumable_upload(bucket, src_path, self.source_path)
            if self.spec_path != None:
                src_path = pre_path + self.spec_path
                oss2.resumable_upload(bucket, src_path, self.spec_path)
            if self.resource_path != None:
                src_path = pre_path + self.resource_path
                oss2.resumable_upload(bucket, src_path, self.resource_path)
            if self.parameters_path != None:
                src_path = pre_path + self.parameters_path
                oss2.resumable_upload(bucket, src_path, self.parameters_path)

    @classmethod
    def query(cls,
              namespace: str,
              name: str = None,
              version: str = None,
              domain: str = "http://127.0.0.1:8080",
              down_load: bool = False) -> list:
        url = domain + "/api/v1/data"
        d = {"namespace": namespace, "name": name, "version": version}
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
        if down_load:
            bucket = get_bucket()
            prefix = "registryCentre/data/"
            for data in res:
                pre_path = prefix + data.namespace + "/" + data.name + "_" + data.version + "/"
                if data.source_path != None and data.source_path != "":
                    src_path = pre_path + data.source_path
                    oss2.resumable_download(bucket, src_path,
                                            data.source_path + "_download")
                if data.spec_path != None and data.spec_path != "":
                    src_path = pre_path + data.spec_path
                    oss2.resumable_download(bucket, src_path,
                                            data.src_path + "_download")
                if data.resource_path != None and data.resource_path != "":
                    src_path = pre_path + data.resource_path
                    oss2.resumable_download(bucket, src_path,
                                            data.resource_path + "_download")
                if data.parameters_path != None and data.parameters_path != "":
                    src_path = pre_path + data.parameters_path
                    oss2.resumable_download(bucket, src_path,
                                            data.parameters_path + "_download")
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
                 docker_image: str = None) -> None:
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

    def insert(self,
               domain: str = "http://127.0.0.1:8080",
               oss_flag: bool = False):
        url = domain + "/api/v1/workflow"
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

    @classmethod
    def query(cls,
              namespace: str,
              name: str = None,
              version: str = None,
              domain: str = "http://127.0.0.1:8080") -> list:
        url = domain + "/api/v1/workflow"
        d = {"namespace": namespace, "name": name, "version": version}
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
                 execute: dict = None) -> None:
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

    def insert(self,
               domain: str = "http://127.0.0.1:8080",
               oss_flag: bool = False):
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

    @classmethod
    def query(cls,
              namespace: str,
              name: str = None,
              version: str = None,
              domain: str = "http://127.0.0.1:8080") -> list:
        url = domain + "/api/v1/OP"
        d = {"namespace": namespace, "name": name, "version": version}
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


def get_bucket() -> oss2.Bucket:
    oss_access_key_id = os.getenv("oss_access_key_id")
    oss_access_key_secret = os.getenv("oss_access_key_secret")
    auth = oss2.Auth(oss_access_key_id, oss_access_key_secret)
    oss_bucket_name = os.getenv("oss_bucket_name")
    oss_end_point = os.getenv("oss_end_point")
    return oss2.Bucket(auth, oss_end_point, oss_bucket_name)


'''
# use like this
m = Model("test_namespace_v1.1",
          "test_name_v1.1",
          "v1.0.5",
          parameters_path="input.json")

# m.insert(oss_flag=True)

r = get_model("test_namespace_v1.1",
              "test_name_v1.1",
              "v1.0.5",
              down_load=True)
print(r[0].id)
'''
