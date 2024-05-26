class WikiIdChecker:
    def __init__(self):
        self._id_dict = {}

    def regist_id(self, id, file, title):
        assert not self.has(id), self.print_error_msg(id, file, title)
        self._id_dict[id] = (file, title)

    def has(self, id):
        return id in self._id_dict

    def print_error_msg(self, id, file, title):
        registed_file = self._id_dict[id][0]
        print(
            f"Registing {title} of {file} failed because {title} of {registed_file} was registed."
        )
