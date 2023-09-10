import os

class ArchFolder:
    
    def __init__(self) -> None:
        datasets_folder = "datasets"
        list_datasets = os.listdir(datasets_folder)
        arch_folder = {}
        for folder in list_datasets:
          arch_folder['folder'] = folder
          arch_folder[folder] = os.listdir(f"datasets/{folder}")
        del arch_folder['folder']
        print('self.arch_paths to get dict, self.tables_path to get all paths')
        self.arch_paths = arch_folder
        self.get_paths()
      
    def get_paths(self):
        self.tables_path = []
        for keys in self.arch_paths:
            for files in self.arch_paths[keys]:
              self.tables_path.append(f'datasets/{keys}/{files}')
        self.tables_path