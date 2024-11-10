# Note that for autograder reasons,
# the following variable must
# be spelled exactly PORT!
PORT ?= 9000
# Default min port=9000 if
# not provided
# Compile command:
# make compile
compile:
# Nothing to compile
# Run command:
# make primary PORT=9020
primary:
	python3 -u server1.py $(PORT)
# Run command:
# make secondary PORT=9020
secondary:
	python3 -u server2.py $(PORT)
# Run command:
# make client1 PORT=9020
client1:
	python3 -u client1.py $(PORT)
# Run command:
# make client2 PORT=9020
client2:
	python3 -u client2.py $(PORT)
# Run command:
# make client3 PORT=9020
client3:
	python3 -u client3.py $(PORT)