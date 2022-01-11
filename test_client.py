import time


from szrpc import log
from szrpc.client import Client

if __name__ == '__main__':

    client = Client('tcp://localhost:9990')

    log.log_to_console()

    def on_done(res, data):
        print("done", res, data)

    def on_err(res, data):
        print("err", res, data)

    def on_update(res, data):
        print("update", res, data)

    def run_one(client, i):
        res = client.process_mx(
            directory=f'/data/Xtal/643/proc-{i}',
            file_names=['/data/Xtal/643/A1_2_00305.cbf'],
            user_name='michel'
        )
        res.connect('done', on_done)
        res.connect('failed', on_err)
        res.connect('update', on_update)
        return True

    time.sleep(5)
    count=3
    while True:
        run_one(client, count)
        count += 1
        time.sleep(30000)

