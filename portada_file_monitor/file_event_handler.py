import json
import os
import redis
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler


class PortadaIngestionEventHandler(FileSystemEventHandler):
    def __init__(self):
        self.file_process_function = None
        self.path_to_observe = None
        self.observer = None

    def set_observer(self, observer):
        self.observer = observer
        return self

    def set_path_to_observe(self, path_to_observe):
        self.path_to_observe = path_to_observe
        return self

    def set_file_process_function(self, process_function):
        self.file_process_function = process_function
        return self

    def start(self):
        self.observer = Observer()
        self.observer.schedule(self, self.path_to_observe, recursive=True)
        self.observer.start()
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
        file_type, user_or_entity = self.get_file_type_and_user_or_entity(event.src_path)
        self.file_process_function(event.src_path, file_type, user_or_entity)

    def get_file_type_and_user_or_entity(self, path_file):
        parents = os.path.dirname(path_file)
        type_and_user = os.path.relpath(parents, self.path_to_observe)
        f_type, user_or_entity = type_and_user.split("/")
        if not f_type:
            f_type = "entry"
        if not user_or_entity:
            user_or_entity = "UNKNOWN_USER"
        return f_type, user_or_entity


class QueuedPortadaIngestionEventHandler(PortadaIngestionEventHandler):
    def __init__(self, host, port, db=2):
        self.redis_queue = r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.queue_name="ingestion_queue"
        self.can_process = True
        self.file_process_function = self.handler_file_process_function
        self.__file_process_function = None

    def set_file_process_function(self, process_function):
        self.__file_process_function = process_function
        return self

    def try_to_process_file(self):
        # 1. Mirem si el sistema està lliure
        if self.can_process:
            p = self.redis_queue.lpop(self.queue_name)

            if p:
                pdict = json.loads(p)
                self.can_process = False
                self.__file_process_function(pdict["path"], pdict["file_type"], pdict["user_or_entity"])
            else:
                print(" [Info] Cua buida, sistema lliure.")

    def handler_file_process_function(self, path, file_type, user_or_entity):
        queue_entry = json.dumps({"path": path, "file_type": file_type, "user_or_entity": user_or_entity})
        self.redis_queue.rpush(self.queue_name, queue_entry)
        self.try_to_process_file()

    def on_deleted(self, event):
        # L'esdeveniment clau: EL SENYAL DE "LLIURE"
        print(f" [-] File processed and removed: {event.src_path}")
        self.can_process = True
        self.try_to_process_file()