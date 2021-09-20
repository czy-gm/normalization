import subprocess
import os
os.system("sync")
os.system(" echo 3 > /proc/sys/vm/drop_caches")
scores=[]
ITERS=10
for i in range(0,ITERS):
    p = subprocess.Popen("perf stat java -jar -Xms2024m -Xmx2024m -Xmn1024m normalization.jar", shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    for line in iter(p.stdout.readline, 'b'):
        # for line in p.stdout.readlines():
        line = line.decode().strip()
        if "seconds time elapsed" in line:
            scores.append(float(line.split(' ')[0]) * 1000)
            break
max_time=max(scores)
min_time=min(scores)
avg_time=(sum(scores)-max_time-min_time)/8.0
print("ITERS:",ITERS)
print("max_time:",max_time)
print("min_time:",min_time)
print("avg_time:",avg_time)