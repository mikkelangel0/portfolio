class ThreadManager:
    def __init__(self):
        self.threads = []

    def add_thread(self, thread):
        self.threads.append(thread)

    def add_threads(self, threads):
        for thread in threads:
            self.add_thread(thread)

    def start_all(self):
        for thread in self.threads:
            thread.start()

    def stop_all(self):
        for thread in self.threads:
            thread.stop()
        print("All threads stopped successfully...")

    def join_all(self):
        for thread in self.threads:
            thread.join(timeout=1)

    def are_all_stopped(self):
        return all(not thread.is_alive() for thread in self.threads)