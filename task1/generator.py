import mmap
import time
import threading
import numpy as np


def random_unsigned(size=250000):
    return np.random.randint(0, 4294967295, size=(size,), dtype=np.uint32)


def generate_file(size=2000):
    with open("numbers.bin", 'ab') as f:
        for _ in range(size):
            data = random_unsigned()
            f.write(data.tobytes())


def read_file(size=2000):
    min_number = 4294967295
    max_number = 0
    sum_ = 0
    with open("numbers.bin", 'rb') as g:
        for _ in range(size):
            buffer = g.read(4 * 250000)
            batch = np.frombuffer(buffer, dtype=np.uint32)
            max_number = max(max_number, batch.max())
            min_number = min(min_number, batch.min())
            sum_ += batch.sum()
    return min_number, max_number, sum_


def read_mmap(size=2000):
    lock = threading.Lock()
    data = []
    nthreads = 4

    def reading(mmaped, start):
        tmin = 4294967295
        tmax = 0
        tsum = 0
        mmaped.seek(start)
        for _ in range(size // nthreads):
            # print(mmaped.tell())
            buffer = mmaped.read(4 * 250000)
            batch = np.frombuffer(buffer, dtype=np.uint32)
            tmax = max(tmax, batch.max())
            tmin = min(tmin, batch.min())
            tsum += batch.sum()
        with lock:
            data.append((tmax, tmin, tsum))

    min_number = 4294967295
    max_number = 0
    sum_ = 0
    with open("numbers.bin", 'rb') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            for i in range(nthreads):
                thread = threading.Thread(target=reading, args=(mm, size * 1000000 // nthreads * i))
                thread.start()
                thread.join()

    for datum in data:
        if datum[0] > max_number:
            max_number = datum[0]
        if datum[1] < min_number:
            min_number = datum[1]
        sum_ += datum[2]

    return min_number, max_number, sum_


if __name__ == "__main__":
    # generate_file(2000)  # generating 2Gb file

    t = time.time()
    print(read_file(2000))
    print(time.time() - t)  # 61.38860511779785 seconds

    t = time.time()
    print(read_mmap(2000))
    print(time.time() - t)  # 38.02911376953125 seconds
