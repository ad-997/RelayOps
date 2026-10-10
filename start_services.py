"""Supervise both processes; fail the container if either service exits."""
import os, signal, subprocess, sys, time
processes=[]
def stop(*args):
    for p in processes:
        if p.poll() is None:p.terminate()
    for p in processes:
        try:p.wait(timeout=10)
        except subprocess.TimeoutExpired:p.kill()
    sys.exit(0 if args else 1)
signal.signal(signal.SIGTERM,stop)
signal.signal(signal.SIGINT,stop)
processes.append(subprocess.Popen([sys.executable,'-m','uvicorn','app:app','--host','127.0.0.1','--port','8082']))
processes.append(subprocess.Popen(['java','-Xms64m','-Xmx192m','-jar','gateway.jar','--server.address=0.0.0.0']))
while True:
    if any(p.poll() is not None for p in processes):stop()
    time.sleep(.5)
