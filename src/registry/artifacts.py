class HTTPArtifact:
    def __init__(self, url):
        self.url = url


class S3Artifact:
    def __init__(self, endpoint, bucket, key, access_key=None,
                 secret_key=None):
        self.endpoint = endpoint
        self.bucket = bucket
        self.key = key
        self.access_key = access_key
        self.secret_key = secret_key


class OSSArtifact:
    def __init__(self, endpoint, bucket, key, access_key=None,
                 secret_key=None):
        self.endpoint = endpoint
        self.bucket = bucket
        self.key = key
        self.access_key = access_key
        self.secret_key = secret_key


class GitArtifact:
    def __init__(self, repo, revision):
        self.repo = repo
        self.revision = revision


class LocalArtifact:
    def __init__(self, path):
        self.path = path
