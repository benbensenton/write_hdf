from hdf import write_hdf as hdf
import time

t0 = time.monotonic()

hdf()

t1 = time.monotonic()
elapsed = t1-t0
print(f'...done, executed in: {elapsed}s')

