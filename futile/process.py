try:
    import prctl
except ImportError:
    prctl = None
import signal
import sys
import os


def run_process(target, *args, auto_quit=True, **kwargs):
    try:
        pid = os.fork()
    except OSError:
        print("unable to fork")
        sys.exit(-1)
    if pid:
        return
    else:
        if auto_quit and prctl:
            prctl.set_pdeathsig(signal.SIGTERM)
        try:
            target(*args, **kwargs)
        except KeyboardInterrupt:
            sys.stderr.write("process exiting...\n")
            sys.stderr.flush()
        sys.exit()

