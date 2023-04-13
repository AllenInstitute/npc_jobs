import tempfile

import np_queuey

with tempfile.mkdtemp() as tempdir:
    huey = np_queuey.HueyQueue(tempdir