from typing import List, Tuple, Set

class File:
    def __init__(self, name, content="", permissions: Set[str]={"r", "w"}):
        self.name = name
        self.content = content
        self.permissions = permissions

    def size(self):
        return len(self.content)
    
    def read(self):
        if "r" not in self.permissions:
            raise PermissionError("Read permission denied.")
        return self.content

    def write(self, content):
        if "w" not in self.permissions:
            raise PermissionError("Write permission denied")
        old_size = self.size()
        self.content = content
        return len(content - old_size) # return delta change

class Directory:
    def __init__(self, name: str, permissions={"r", "w", "x"}):
        self.name = name
        self.subdirs = {}   # name -> directory
        self.files = {}     # name -> File
        self.permissions = permissions
        self.quota = None
        self.usage = 0
        self.parent = None

    def list_contents(self) -> List[str]:
        self.check("r")
        return sorted(list(self.subdirs.keys()) + list(self.files.keys()))
    
    def check(self, perm):
        if perm not in self.permissions:
            raise PermissionError(f"{perm.upper()} permission denied on directory '{self.name}'.")
        
    def update_usage(self, delta):
        if self.quota is not None and self.usage + delta > self.quota:
            raise MemoryError(f"Quota exceeded in directory '{self.name}'. Limit: {self.quota} bytes")
        self.usage += delta
        if self.parent:
            self.parent.update_usage(delta)

    
class FileSystem:
    def __init__(self):
        self.root = Directory("/")

    def _traverse(self, path, for_write=False) -> Tuple[Directory, str]:
        parts = [p for p in path.strip("/").split("/") if p]
        curr = self.root
        for part in parts[:-1]:
            curr.check("x")
            if part in curr.subdirs:
                curr = curr.subdirs[part]
            else:
                raise FileNotFoundError(f"Directory '{part}' not found.")
            curr.check("x")
            if for_write:
                curr.check("w")
        return curr, parts[-1] if parts else ""
    
    def mkdir(self, path):
        parts = [p for p in path.strip("/").split("/") if p]
        curr = self.root
        for part in parts:
            curr.check("x")
            if part not in curr.subdirs:
                curr.check("w")
                new_dir = Directory(part)
                new_dir.parent = curr
                curr.subdirs[part] = new_dir
            curr = curr.subdirs[part]

    def add_file(self, path, content) -> None:
        dir_node, filename = self._traverse(path, True)
        if filename in dir_node.files:
            raise FileExistsError(f"File '{filename}' already exists.")
        size = len(content)
        dir_node.update_usage(size)
        dir_node.files[filename] = File(filename, content)

    def ls(self, path):
        # print("############", path)
        if path == "/":
            return self.root.list_contents()
        node, name = self._traverse(path)
        if name in node.files:
            return [name]
        elif name in node.subdirs:
            # print(node.name, node.subdirs, name)
            return node.subdirs[name].list_contents()
        else:
            raise FileNotFoundError("Path does not exist.")
        
    def read_file(self, path):
        node, filename = self._traverse(path)
        if filename in node.files:
            return node.files[filename].read()
        raise FileNotFoundError("File not found.")
    
    def set_quota(self, path, limit_bytes):
        node, name = self._traverse(path)
        if name in node.subdirs:
            node.subdirs[name].quota = limit_bytes
        elif path == "/":
            self.root.quota = limit_bytes
        else:
            raise FileNotFoundError("Directory not found.")

    def get_usage(self, path):
        node, name = self._traverse(path)
        if name in node.subdirs:
            return node.subdirs[name].usage
        elif path == "/":
            return self.root.usage
        else:
            raise FileNotFoundError("Directory not found.")
        
    def chmod(self, path, permissions):
        node, name = self._traverse(path)

        # Target is a file
        if name in node.files:
            node.files[name].permissions = set(permissions)
        # Target is a subdirectory
        elif name in node.subdirs:
            node.subdirs[name].permissions = set(permissions)
        # Might be root
        elif path == "/":
            self.root.permissions = set(permissions)
        else:
            raise FileNotFoundError(f"No such file or directory: {path}")
    
fs = FileSystem()
fs.mkdir("/a/b")
fs.add_file("/a/b/file.txt", "hello world")
print(fs.ls("/a"))             # ['b']
print(fs.ls("/a/b"))           # ['file.txt']
print(fs.read_file("/a/b/file.txt"))  # "hello world"

fs = FileSystem()
fs.mkdir("/docs")
fs.add_file("/docs/readme.txt", "hello")

# Remove read permission from readme
fs.chmod("/docs/readme.txt", {"w"})
try:
    fs.read_file("/docs/readme.txt")  # ❌ raises PermissionError
except Exception as e:
    print(e)

fs = FileSystem()
fs.mkdir("/a/b")
fs.set_quota("/a", 10)

fs.add_file("/a/b/x.txt", "12345")   # OK (5 bytes)
print(fs.get_usage("/a"))            # 5

fs.add_file("/a/b/y.txt", "123456")  # ❌ raises MemoryError