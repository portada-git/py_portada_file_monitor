import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dagster_graphql import DagsterGraphQLClient


class PortadaIngestionEventHandler(FileSystemEventHandler):
    """Classe que defineix què fer quan hi ha canvis."""

    def __init__(self):
        self.client = None
        self.path_to_observe = None
        self.observer = None

    def set_observer(self, observer):
        self.observer = observer
        return self

    def set_path_to_observe(self, path_to_observe):
        self.path_to_observe = path_to_observe
        return self

    def start(self):
        self.observer = Observer()
        self.observer.schedule(self, self.path_to_observe, recursive=True)
        self.observer.start()
        self.client = DagsterGraphQLClient(hostname="localhost", port_number=3000)
        print(f"Monitor iniciat a: {self.path_to_observe}")


    def stop(self):
        self.observer.stop()
        self.observer.join()
        print("Monitor aturat correctament.")


    def on_created(self, event):
        # Ignorem si és un directori
        if not event.is_directory:
            print(f"Nou fitxer detectat: {event.src_path}")
            self.process_file(event.src_path)

    def process_file(self, ruta_fitxer):
        parent = os.path.dirname(ruta_fitxer)
        user = os.path.relpath(parent, self.path_to_observe)
        if not user:
            user = "UNKNOWN_USER"
        self.client.submit_job_execution(
            job_name="ingestion",
            run_config={
                "ops": {"ingested_entry_file": {"config": {"local_path": ruta_fitxer, "user": user}}},
                "resources": {
                    "datalayer": {
                        "config": {
                            "config_path": ruta_fitxer,
                        }
                    }
                }
            }
        )


if __name__ == "__main__":
    PATH_A_VIGILAR = "./la_teva_carpeta"  # Canvia-ho pel teu directori

    # Creem l'observador i el manejador
    event_handler = PortadaIngestionEventHandler()
    event_handler.set_path_to_observe(PATH_A_VIGILAR).set_observer(Observer()).start()

    try:
        while True:
            # El procés principal es queda aquí esperant
            time.sleep(1)
    except KeyboardInterrupt:
        event_handler.stop()
        print("\nMonitorització aturada.")

