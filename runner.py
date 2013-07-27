import subprocess
for i in range(2,3):
	subprocess.call(["./run.sh %d" % i])