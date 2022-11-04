class Artifact:
    @staticmethod
    def from_dict(d):
        if "http" in d:
            return HTTPArtifact(**d["http"])
        elif "git" in d:
            return GitArtifact(**d["git"])


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
