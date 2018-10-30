import prctl
import signal
import sys
import os


def run_process(target, *, auto_quit=True):
    try:
        pid = os.fork()
    except OSError:
        print("unable to fork")
        sys.exit(-1)
    if pid:
        return
    else:
        if auto_quit:
            prctl.set_pdeathsig(signal.SIGTERM)
        try:
            target()
        except KeyboardInterrupt:
            sys.stderr.write("process exiting...\n")
            sys.stderr.flush()
        sys.exit()

