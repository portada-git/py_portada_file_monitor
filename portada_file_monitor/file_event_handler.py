# import time
import os
import requests
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler
# from dagster_graphql import DagsterGraphQLClient


class PortadaIngestionEventHandler(FileSystemEventHandler):
    """Classe que defineix què fer quan hi ha canvis."""

    def __init__(self):
        # self.entry_process_function = self.dagster_process_entry
        self.host = "localhost"
        # self.client = None
        self.path_to_observe = None
        self.observer = None
        # self.data_layer_config_path = None
        self.port = 5555

    def set_observer(self, observer):
        self.observer = observer
        return self

    def set_path_to_observe(self, path_to_observe):
        self.path_to_observe = path_to_observe
        return self

    # def set_data_layer_config_path(self, data_layer_config_path):
    #     self.data_layer_config_path = data_layer_config_path
    #     return self

    def set_host(self, host):
        self.host = host
        return self


    def set_port(self, port):
        self.port = port
        return self

    # def set_entry_file_process(self, process_function):
    #     self.entry_process_function = process_function
    #     return self

    def start(self):
        self.observer = Observer()
        self.observer.schedule(self, self.path_to_observe, recursive=True)
        self.observer.start()
        # self.client = DagsterGraphQLClient(hostname=self.host, port_number=3000)
        print(f"Monitor iniciat a: {self.path_to_observe}")


    def stop(self):
        self.observer.stop()
        self.observer.join()
        print("Monitor aturat correctament.")

    def on_created(self, event):
        print(f"DEBUG: Esdeveniment rebut: {event.src_path} - IsDir: {event.is_directory}")
        if event.is_directory or os.path.isdir(event.src_path):
            print(f"DEBUG: Ignorat perquè és un directori: {event.src_path}")
            return
        print(f"Nou fitxer detectat: {event.src_path}")
        self.process_file(event.src_path)

    def process_file(self, ruta_fitxer):
        parents = os.path.dirname(ruta_fitxer)
        type_and_user = os.path.relpath(parents, self.path_to_observe)
        f_type, user_or_entity = type_and_user.split("/")
        if not f_type:
            f_type = "entry"
        if not user_or_entity:
            user_or_entity = "UNKNOWN_USER"
        if f_type.lower() == "entity":
            url = f"http://{self.host}:{self.port}/entity/ingestion"
            params = {
                "file_path": ruta_fitxer,
                "entity":user_or_entity
            }
        else:
            url = f"http://{self.host}:{self.port}/entry/ingestion"
            params = {
                "file_path": ruta_fitxer,
                "user":user_or_entity
            }
        print(f"DEBUG: url: {url}")
        try:
            response = requests.post(url, json=params)
            print(response.json())
        except Exception as e:
            print(f"DEBUG: error connecting to {url}. Error message: {e}")

    # @staticmethod
    # def dagster_process_entry(self, ruta_fitxer, user):
    #     self.client.submit_job_execution(
    #         job_name="ingestion",
    #         run_config={
    #             "ops": {"ingested_entry_file": {"config": {"local_path": ruta_fitxer, "user": user}}},
    #             "resources": {
    #                 "datalayer": {
    #                     "config": {
    #                         "config_path": self.data_layer_config_path,
    #                     }
    #                 }
    #             }
    #         }
    #     )


