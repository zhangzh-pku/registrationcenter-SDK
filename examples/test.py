import os
import sys
from dflow import s3_config
from dflow.plugins.bohrium import config, TiefblueClient

path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, path)

from src.registry import LocalPath, Dataset

config["username"] = os.getenv("bohrium_user")
config["password"] = os.getenv("bohrium_pswd")
config["project_id"] = 10270
config["bohrium_url"] = os.getenv("bohrium_url")
s3_config["repo_key"] = os.getenv("s3_repo_key")
s3_config["storage_client"] = TiefblueClient()


# junk.data可以自己生成
# 例如 dd if=/dev/zero of=junk.data bs=1M count=1
# 可以生成1M大小的文件
tem = LocalPath("junk.data")
data = Dataset(namespace="test", name="junk", version="v1.0.0.4", location=tem)

data.insert(upload=True)

print(data.id)

res = Dataset.query(id=data.id, down_load=True)
print(res[0])
