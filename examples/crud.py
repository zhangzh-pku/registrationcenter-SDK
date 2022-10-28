import os
import sys

path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, path)

from src.registry import Model, Dataset, OP, Workflow
from src.registry.artifacts import OSSArtifact

access_key_id = os.getenv("oss_access_key_id")
access_key_secret = os.getenv("oss_access_key_secret")
bucket_name = os.getenv("oss_bucket_name")
end_point = os.getenv("oss_end_point")

tem = OSSArtifact(end_point,
                  bucket_name,
                  access_key_id,
                  secret_key=access_key_secret)

localhost = "http://127.0.0.1:8080"


def model_insert_list():
    for i in range(10):
        version = "v1.0.0." + str(i)
        m = Model(namespace="test_namespace_v1",
                  name="test_name",
                  version=version,
                  location=tem)
        m.insert()
        res = Model.query(id=m.id)[0]
        print(f"{res.namespace}/{res.name}:{res.version} id:{res.id}")


def model_query_namespace():
    res_list = Model.query(domain=localhost, namespace="test_*")
    for res in res_list:
        print(f"{res.namespace}/{res.name}:{res.version} id:{res.id}")


def data_insert_list():
    for i in range(10):
        version = "v1.0.0." + str(i)
        d = Dataset(namespace="test_namespace_v1",
                    name="test_name",
                    version=version,
                    location=tem)
        d.insert()
        res = Dataset.query(domain=localhost, id=d.id)[0]
        print(f"{res.namespace}/{res.name}:{res.version} id:{res.id}")


def data_query_namespace():
    res_list = Dataset.query(domain=localhost, namespace="test_*")
    for res in res_list:
        print(f"{res.namespace}/{res.name}:{res.version} id:{res.id}")


def op_insert_list():
    for i in range(10):
        version = "v1.0.0." + str(i)
        op = OP(namespace="test_namespace_v1",
                name="test_name",
                version=version)
        op.insert()
        res = OP.query(id=op.id)[0]
        print(f"{res.namespace}/{res.name}:{res.version} id:{res.id}")


def op_query_namespace():
    res_list = OP.query(domain=localhost, namespace="test_*")
    for res in res_list:
        print(f"{res.namespace}/{res.name}:{res.version} id:{res.id}")


def wf_insert_list():
    for i in range(10):
        version = "v1.0.0." + str(i)
        wf = Workflow(namespace="test_namespace_v1",
                      name="test_name",
                      version=version)
        wf.insert()
        res = Workflow.query(id=wf.id)[0]
        print(f"{res.namespace}/{res.name}:{res.version} id:{res.id}")


def wf_query_namespace():
    res_list = Workflow.query(domain=localhost, namespace="test_*")
    for res in res_list:
        print(f"{res.namespace}/{res.name}:{res.version} id:{res.id}")


def model_query_latest():
    res_list = Model.query(namespace="test_namespace_v1", version="latest")
    for res in res_list:
        print(f"model:{res.namespace}/{res.name}:{res.version} id:{res.id}")


def data_query_latest():
    res_list = Dataset.query(namespace="test_namespace_v1", version="latest")
    for res in res_list:
        print(f"data:{res.namespace}/{res.name}:{res.version} id:{res.id}")


def op_query_latest():
    res_list = OP.query(namespace="test_namespace_v1", version="latest")
    for res in res_list:
        print(f"op:{res.namespace}/{res.name}:{res.version} id:{res.id}")


def wf_query_latest():
    res_list = Workflow.query(namespace="test_namespace_v1", version="latest")
    for res in res_list:
        print(f"workflow:{res.namespace}/{res.name}:{res.version} id:{res.id}")


def model_insert_list_name_version():
    for i in range(3):
        for j in range(3):
            version = "v1.0.0." + str(i)
            m = Model(namespace="test_namespace_v1",
                      name="test_name_" + str(j),
                      version=version,
                      location=tem)
            m.insert()
            res = Model.query(id=m.id)[0]
            print(
                f"model:{res.namespace}/{res.name}:{res.version} id:{res.id}")


def data_insert_list_name_version():
    for i in range(3):
        for j in range(3):
            version = "v1.0.0." + str(i)
            m = Dataset(namespace="test_namespace_v1",
                        name="test_name_" + str(j),
                        version=version,
                        location=tem)
            m.insert()
            res = Dataset.query(id=m.id)[0]
            print(f"data:{res.namespace}/{res.name}:{res.version} id:{res.id}")


def wf_insert_list_name_version():
    for i in range(3):
        for j in range(3):
            version = "v1.0.0." + str(i)
            m = Workflow(namespace="test_namespace_v1",
                         name="test_name_" + str(j),
                         version=version)
            m.insert()
            res = Workflow.query(id=m.id)[0]
            print(
                f"workflow:{res.namespace}/{res.name}:{res.version} id:{res.id}"
            )


def op_insert_list_name_version():
    for i in range(3):
        for j in range(3):
            version = "v1.0.0." + str(i)
            m = OP(namespace="test_namespace_v1",
                   name="test_name_" + str(j),
                   version=version)
            m.insert()
            res = OP.query(id=m.id)[0]
            print(f"op{res.namespace}/{res.name}:{res.version} id:{res.id}")
