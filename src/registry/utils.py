import hashlib


def check_md5(path: str):
    m = hashlib.md5()
    with open(path, 'rb') as fobj:
        while True:
            data = fobj.read(4096)
            if not data:
                break
            m.update(data)
    return m.hexdigest()