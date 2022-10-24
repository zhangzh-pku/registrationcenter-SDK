class Artifact:
    @staticmethod
    def from_dict(d):
        if "http" in d:
            return HTTPArtifact(**d["http"])
        elif "s3" in d:
            return S3Artifact(**d["s3"])
        elif "oss" in d:
            return OSSArtifact(**d["oss"])
        elif "git" in d:
            return GitArtifact(**d["git"])


class HTTPArtifact(Artifact):
    def __init__(self, url, **kwargs):
        self.url = url

    def to_dict(self):
        return {"http": self.__dict__}


class S3Artifact(Artifact):
    def __init__(self, endpoint, bucket, key, access_key=None,
                 secret_key=None, **kwargs):
        self.endpoint = endpoint
        self.bucket = bucket
        self.key = key
        self.access_key = access_key
        self.secret_key = secret_key

    def to_dict(self):
        return {"s3": self.__dict__}


class OSSArtifact(Artifact):
    def __init__(self, endpoint, bucket, key, access_key=None,
                 secret_key=None, **kwargs):
        self.endpoint = endpoint
        self.bucket = bucket
        self.key = key
        self.access_key = access_key
        self.secret_key = secret_key

    def to_dict(self):
        return {"oss": self.__dict__}


class GitArtifact(Artifact):
    def __init__(self, repo, revision, **kwargs):
        self.repo = repo
        self.revision = revision

    def to_dict(self):
        return {"git": self.__dict__}


class LocalPath:
    def __init__(self, path):
        self.path = path
