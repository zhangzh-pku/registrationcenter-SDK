import json
import os

import oss2
import requests


class Model:

    def __init__(self,
                 namespace: str,
                 name: str,
                 version: str,
                 description: str = None,
                 readme: str = None,
                 author: str = None,
                 labels: dict = None,
                 status: str = None,
                 size: int = None,
                 location: dict = None,
                 code: dict = None,
                 source_path: str = None,
                 parameters_path: str = None,
                 spec_path: str = None,
                 resources_path: str = None) -> None:
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
        self.source_path = source_path
        self.parameters_path = parameters_path
        self.spec_path = spec_path
        self.resource_path = resources_path

    # 默认只保存路径到注册中心，oss为True时上传source、parameters、spec、resource到oss平台
    # 如果要上传到oss，那么必须要在系统环境变量中配置相关环境变量
    # 具体细节见使用文档
    def insert(self,
               domain: str = "http://127.0.0.1:8080",
               oss_flag: bool = False):
        url = domain + "/api/v1/model"
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
        if oss_flag:
            bucket = get_bucket()
            #path = [p for p in d if "_path" in p and p is not None]
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


class Data:

    def __init__(self,
                 namespace: str,
                 name: str,
                 version: str,
                 description: str = None,
                 readme: str = None,
                 author: str = None,
                 labels: dict = None,
                 status: str = None,
                 size: int = None,
                 location: dict = None,
                 code: dict = None,
                 source_path: str = None,
                 parameters_path: str = None,
                 spec_path: str = None,
                 resources_path: str = None) -> None:
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
        self.source_path = source_path
        self.parameters_path = parameters_path
        self.spec_path = spec_path
        self.resource_path = resources_path

    def insert(self,
               domain: str = "http://127.0.0.1:8080",
               oss_flag: bool = False):
        url = domain + "/api/v1/data"
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
        if oss_flag:
            bucket = get_bucket()
            #path = [p for p in d if "_path" in p and p is not None]
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


class workflow:

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


def get_bucket() -> oss2.Bucket:
    oss_access_key_id = os.getenv("oss_access_key_id")
    oss_access_key_secret = os.getenv("oss_access_key_secret")
    auth = oss2.Auth(oss_access_key_id, oss_access_key_secret)
    oss_bucket_name = os.getenv("oss_bucket_name")
    oss_end_point = os.getenv("oss_end_point")
    return oss2.Bucket(auth, oss_end_point, oss_bucket_name)


def get_model(namespace: str,
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
        # marshal
        try:
            name = mod["name"]
            namespace = mod["namespace"]
            version = mod["version"]
        except KeyError:
            continue
        m = Model(namespace, name, version)
        for k in mod:
            m.__setattr__(k, mod[k])
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


def get_data(namespace: str,
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
        try:
            name = data["name"]
            namespace = data["namespace"]
            version = data["version"]
        except KeyError:
            continue
        d = Data(namespace, name, version)
        for k in data:
            d.__setattr__(k, data[k])
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


def get_op(namespace: str,
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
        o = OP(namespace, name, version)
        for k in op:
            o.__setattr__(k, op[k])
        res.append(o)
    return res


def get_workflow(namespace: str,
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
        w = workflow(namespace, name, version)
        for k in wf:
            w.__setattr__(k, wf[k])
        res.append(w)
    return res


'''
# use like this
m = Model("test_namespace_v1.1",
          "test_name_v1.1",
          "v1.0.5",
          parameters_path="input.json")

#m.insert(oss_flag=True)

r = get_model("test_namespace_v1.1",
              "test_name_v1.1",
              "v1.0.5",
              down_load=True)
print(r[0].id)
'''