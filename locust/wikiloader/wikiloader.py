import os
import json
from id_checker import WikiIdChecker


class WikiJsonIterator:
    def __init__(self, root_dir):
        if not os.path.exists(root_dir):
            raise ValueError(f"指定されたディレクトリが存在しません: {root_dir}")
        self.root_dir = root_dir
        self.current_dir_index = 0
        self.current_file_index = 0
        self.current_file = None
        self.dir_paths = self._gather_dir_paths()
        self.file_paths = self._gather_file_paths()
        self._id_checker = WikiIdChecker()
        self._cnt = 0

    def _gather_dir_paths(self):
        dir_paths = []
        for dirpath, dirnames, filenames in os.walk(self.root_dir):
            for dirname in dirnames:
                dir_paths.append(os.path.join(dirpath, dirname))
        return dir_paths

    def _gather_file_paths(self):
        file_paths = []
        for dirpath, dirnames, filenames in os.walk(
            self.dir_paths[self.current_dir_index]
        ):
            for filename in filenames:
                if filename.startswith("wiki_"):
                    file_paths.append(os.path.join(dirpath, filename))
        return sorted(file_paths)

    def _open_next_dir(self):
        if self.current_dir_index < len(self.dir_paths):
            print(self.dir_paths[self.current_dir_index])
            self.file_paths = self._gather_file_paths()
            self.current_dir_index += 1
        else:
            self.current_dir_index = -1

    def _open_next_file(self):
        if self.current_dir_index == -1:
            return
        if self.current_file:
            self.current_file.close()
        if self.current_file_index < len(self.file_paths):
            self.current_file = open(
                self.file_paths[self.current_file_index], "r", encoding="utf-8"
            )
            self.current_file_index += 1
        else:
            self._open_next_dir()
            if self.current_dir_index == -1:  # finish
                self.current_fie_index = -1
                self.current_file = None
                return
            self.current_file_index = 0
            self.current_file = open(
                self.file_paths[self.current_file_index], "r", encoding="utf-8"
            )

    def _read_next_line(self):
        if not self.current_file:
            return None
        line = self.current_file.readline()
        return line if line else None

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if not self.current_file:
                self._open_next_file()
                if not self.current_file:
                    print("Finish")
                    raise StopIteration

            line = self._read_next_line()
            if line:
                jsondict = json.loads(line)
                id = jsondict["id"]
                if self._id_checker.has(id):
                    continue
                else:
                    title = jsondict["title"]
                    self._id_checker.regist_id(id, self.current_file.name, title)
                    self._cnt += 1
                return json.dumps(jsondict, ensure_ascii=False, separators=(", ", ": "))
            else:
                self._open_next_file()

    def print_cnt(self):
        print(f"Article: {self._cnt} records")


if __name__ == "__main__":
    root_dir = "/data/text/"
    wiki_iterator = WikiJsonIterator(root_dir)
    for json_data in wiki_iterator:
        print(json_data)
        break
