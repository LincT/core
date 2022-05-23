import os
from stat import filemode


# noinspection PyBroadException
class FileIO:
    @staticmethod
    def read_as_string(file_name: str) -> str:
        try:
            """read info from file, if file exists. return a string"""
            with open(str(file_name)) as f:
                data = f.read()
                f.close()
                return data
        except FileNotFoundError:
            # we want a string either way, but this way we can let the calling class decide business logic
            return ""

    @staticmethod
    def read_as_pos_int(file_name: str) -> int:
        try:
            """read info from file. If file exists, return a positive int, negative for errors"""
            with open(str(file_name)) as f:
                data = f.read()
                f.close()
                if data.isnumeric():
                    return int(data)
                else:
                    return 0
        except Exception:
            # if there's no file, or the file has invalid data, return -1 for value
            return -1

    @staticmethod
    def mkdir(new_dir: str) -> int:  # returns an int to indicate status, 1 created, 0 already exists, -1 error
        try:
            os.mkdir(str(new_dir))
            return 1
        except FileExistsError:
            return 0
        except Exception:
            return -1

    @staticmethod
    def verify_dir(new_dir: str) -> int:
        try:
            if os.listdir(new_dir) is not []:
                return 1

        except FileNotFoundError:
            return -1

    @staticmethod
    def path_join(directory: str, file: str) -> str:
        # wrapper for path join to eliminate importing os elsewhere
        return os.path.join(str(directory), str(file))

    @staticmethod
    def overwrite(file_name: str, data):
        with open(file_name, 'w') as f:
            f.write(str(data))

    @staticmethod  # not needed in this project, but good to have if we use this class elsewhere
    def append(file_name: str, data):
        with open(str(file_name)) as current:
            data = current.read() + data
            current.close()
        with open(str(file_name), 'w') as f:
            f.write(data)
            f.close()

    @staticmethod
    def long_list_files(target_dir: str = ".") -> list:
        # if no directory specified, should list files where calling file lives.
        dir_list = os.listdir(target_dir)
        data_list = []
        for each in dir_list:
            stat = os.stat(os.path.join(target_dir, each)).st_mode
            mode = filemode(stat)
            data_list.append("\t".join([mode, each]))
        return data_list
