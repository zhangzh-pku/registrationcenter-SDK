from dflow import S3Artifact
import json

class Artifact:
    @staticmethod
    def from_dict(d):
        if "http" in d:
            return HTTPArtifact(**d["http"])
        elif "git" in d:
            return GitArtifact(**d["git"])
        elif "s3" in d:
            return S3Artifact.from_dict(d["s3"])
        elif "s3_dict" in d:
            _d = json.loads(d["s3_dict"])
            res = {}
            for k in _d:
                res[k] = Artifact.from_dict(_d[k])
            return res
        elif "s3_list" in d:
            lis = json.loads(d["s3_list"])
            res = []
            for i in lis:
                res.append(Artifact.from_dict(i))
            return res


class HTTPArtifact(Artifact):
    def __init__(self, url, **kwargs):
        self.url = url

    def to_dict(self):
        return {"http": self.__dict__}


class GitArtifact(Artifact):
    def __init__(self, repo, revision, **kwargs):
        self.repo = repo
        self.revision = revision

    def to_dict(self):
        return {"git": self.__dict__}


class LocalPath:
    def __init__(self, path):
        self.path = path
