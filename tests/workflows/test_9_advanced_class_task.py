# from engine.engine import flow, task

# @task
# def get_content():
#     return "apples"


# class Template:
#     def __init__(self, content: str, image_paths: list[str], csv_paths: list[str]):
#         self.content = content
#         self.image_paths = image_paths
#         self.csv_paths = csv_paths
    
    
#     def add_content(self, text: str): 
#         ...

#     def add_images(self, *image_path: str): 
#         ...
    
#     def add_data(self, *csv_path: str): 
#         ...

#     def compile_project(self, text, images, csvs): 
#         ...
    
#     # A run method must be implemented either in the child or base class
#     def run(self):
#         images = self.add_images(*self.image_paths)
#         data = self.add_data(*self.image_paths)
#         return self.compile_project(self.content, images, data)


# @task
# class MyProject(Template):
#     def __init__(self, content: str, image_paths: list[str], csv_paths: list[str]):
#         super().__init__(content, image_paths, csv_paths)
            
#     def add_data(self, *csv_path: str):
#         print("images get added on each page instead of the default")
        
        
# @flow
# def workflow(image_paths, csv_paths):
#     content = get_content()
#     MyProject(content, image_paths, csv_paths)
    
    
# def test_class_task():
#     workflow('~/home/user/images', '~/home/user/csv')