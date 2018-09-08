def queue_consumer(q, handler):
    while True:
        data = q.get()
        handler(data)
