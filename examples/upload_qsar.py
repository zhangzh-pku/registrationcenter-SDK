import os
import sys
from dflow import s3_config
from dflow.plugins.bohrium import config, TiefblueClient
from src.registry import Dataset, LocalPath

config["username"] = os.getenv("bohrium_user")
config["password"] = os.getenv("bohrium_pswd")
config["project_id"] = 10270
config["tiefblue_url"] = os.getenv("tiefblue_url")
config["bohrium_url"] = os.getenv("bohrium_url")
s3_config["repo_key"] = os.getenv("s3_repo_key")
s3_config["storage_client"] = TiefblueClient()

pre_namespace = "qsar-benchmark"
test = "test.csv"
train = "train.csv"
namespace_list = os.listdir(os.getcwd() + "/" + pre_namespace)
for namespace in namespace_list:
    namespace = pre_namespace + "/" + namespace
    pwd = os.getcwd()
    os.chdir(namespace)
    name_list = os.listdir(os.getcwd())
    for name in name_list:
        _pwd = os.getcwd()
        os.chdir(name)
        data = Dataset(name=name,
                       namespace=namespace,
                       version="v1.0.0.0",
                       location=LocalPath(test))
        data = Dataset(name=name,
                       namespace=namespace,
                       version="v1.0.0.1",
                       location={
                           test: LocalPath(test),
                           train: LocalPath(train)
                       })
        data = Dataset(name=name,
                       namespace=namespace,
                       version="v1.0.0.2",
                       location=[LocalPath(test),
                                 LocalPath(train)])
        data.insert()
        print(data.id)
        os.chdir(_pwd)
    os.chdir(pwd)
