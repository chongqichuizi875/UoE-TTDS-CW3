import threading


class MyThread(threading.Thread):
    def __init__(self, threadId, func, args):
        threading.Thread.__init__(self)
        self.result = None
        self.threadId = threadId
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)
        # print('id: ' + str(self.threadId) + "   result: " + str(self.result))

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None

    def __del__(self):
        print("thread " + str(self.threadId) + " completed.")
