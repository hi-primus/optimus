@classmethod
def delete_check_point_folder(cls, path, file_system):
    """
    Function that deletes the temporal folder where temp files were stored.
    The path required is the same provided by user in setCheckPointFolder().

    :param path:
    :param file_system: Describes if file system is local or hadoop file system.
    :return:
    """

    assert (isinstance(file_system, str)), "Error: file_system argument must be a string."

    assert (file_system == "hadoop") or (file_system == "local"), \
        "Error, file_system argument only can be 'local' or 'hadoop'"

    if file_system == "hadoop":
        # Folder path:
        folder_path = path + "/" + "checkPointFolder"
        print("Deleting checkpoint folder...")
        command = "hadoop fs -rm -r " + folder_path
        os.system(command)
        print("$" + command)
        print("Folder deleted. \n")
    else:
        print("Deleting checkpoint folder...")
        # Folder path:
        folder_path = path + "/" + "checkPointFolder"
        # Checking if tempFolder exits:
        if os.path.isdir(folder_path):
            # Deletes folder if exits:
            rmtree(folder_path)
            # Creates new folder:
            print("Folder deleted. \n")
        else:
            print("Folder deleted. \n")
            pass

def set_check_point_folder(self, path, file_system):
    """
    Function that receives a workspace path where a folder is created.
    This folder will store temporal
    dataframes when user writes the DataFrameTransformer.checkPoint().

    This function needs the sc parameter, which is the spark context in order to
    tell spark where is going to save the temporal files.

    It is recommended that users deleted this folder after all transformations are completed
    and the final dataframe have been saved. This can be done with deletedCheckPointFolder function.

    :param path: Location of the dataset (string).
    :param file_system: Describes if file system is local or hadoop file system.

    """

    assert (isinstance(file_system, str)), \
        "Error: file_system argument must be a string."

    assert (file_system == "hadoop") or (file_system == "local"), \
        "Error, file_system argument only can be 'local' or 'hadoop'"

    if file_system == "hadoop":
        folder_path = path + "/" + "checkPointFolder"
        self.delete_check_point_folder(path=path, file_system=file_system)

        # Creating file:
        print("Creation of hadoop folder...")
        command = "hadoop fs -mkdir " + folder_path
        print("$" + command)
        os.system(command)
        print("Hadoop folder created. \n")

        print("Setting created folder as checkpoint folder...")
        self.__sc.setCheckpointDir(folder_path)
        print("Done.")
    else:
        # Folder path:
        folder_path = path + "/" + "checkPointFolder"
        # Checking if tempFolder exits:
        print("Deleting previous folder if exists...")
        if os.path.isdir(folder_path):

            # Deletes folder if exits:
            rmtree(folder_path)
            print("Creation of checkpoint directory...")
            # Creates new folder:
            os.mkdir(folder_path)
            print("Done.")
        else:
            print("Creation of checkpoint directory...")

            # Creates new folder:
            os.mkdir(folder_path)
            print("Done.")

        self.__sc.setCheckpointDir(dirName="file:///" + folder_path)