import configparser

class Variables():
    def __init__(self, variable_name):
        self.path = "config/config.cfg"
        self.name = variable_name

    def get_variable(self):
        with open(self.path, "r") as file:
            config = configparser.ConfigParser()
            config.read(self.path)
            file_content = dict(config[self.name])
            return file_content

